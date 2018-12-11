/*
 * Copyright 2018 dc-square GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.callbacks;

import com.amazonaws.services.s3.model.*;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.services.cluster.ClusterDiscoveryCallback;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.extensions.aws.S3Client;
import com.hivemq.extensions.config.ClusterNodeFile;
import com.hivemq.extensions.config.ConfigurationReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryCallback implements ClusterDiscoveryCallback {

    private static final Logger logger = LoggerFactory.getLogger(S3DiscoveryCallback.class);

    @NotNull
    S3Client s3Client;

    private ClusterNodeFile ownNodeFile;

    public S3DiscoveryCallback(@NotNull final ConfigurationReader configurationReader) {
        this.s3Client = new S3Client(configurationReader);
    }

    @Override
    public void init(@NotNull final ClusterDiscoveryInput clusterDiscoveryInput, @NotNull final ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            try {
                s3Client.createOrUpdate();
            } catch (final Exception ex) {
                logger.error("Configuration of the S3 discovery extension couldn't be loaded. Skipping initial discovery.");
                return;
            }
            if (!s3Client.doesBucketExist()) {
                logger.error("Configured bucket '{}' doesn't exist. Skipping initial discovery.", s3Client.getS3Config().getBucketName());
                return;
            }

            saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
            clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());
        } catch (final Exception ex) {
            logger.error("Initialization of the S3 discovery callback failed.", ex);
        }
    }

    @Override
    public void reload(@NotNull final ClusterDiscoveryInput clusterDiscoveryInput, @NotNull final ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            try {
                s3Client.createOrUpdate();
            } catch (final Exception ex) {
                logger.error("Configuration of the S3 discovery extension couldn't be reloaded. Skipping reload callback.");
                return;
            }
            if (!s3Client.doesBucketExist()) {
                logger.error("Configured bucket '{}' doesn't exist. Skipping reload callback.", s3Client.getS3Config().getBucketName());
                return;
            }

            if (ownNodeFile == null || ownNodeFile.isExpired(s3Client.getS3Config().getFileUpdateIntervalInSeconds())) {
                saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
            }
            clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());
        } catch (final Exception ex) {
            logger.error("Reload of the S3 discovery callback failed.", ex);
        }
    }

    @Override
    public void destroy(@NotNull final ClusterDiscoveryInput clusterDiscoveryInput) {
        try {
            if (ownNodeFile != null) {
                removeOwnFile(clusterDiscoveryInput.getOwnClusterId());
            }
        } catch (final Exception ex) {
            logger.error("Destroy of the S3 discovery callback failed.", ex);
        }
    }

    private void saveOwnFile(@NotNull final String ownClusterId, @NotNull final ClusterNodeAddress ownAddress) throws Exception {
        final String objectKey = s3Client.getS3Config().getFilePrefix() + ownClusterId;
        final ClusterNodeFile newNodeFile = new ClusterNodeFile(ownClusterId, ownAddress);

        s3Client.saveObject(objectKey, newNodeFile.toString());
        ownNodeFile = newNodeFile;

        logger.debug("Updated own S3 file '{}'.", objectKey);
    }

    private void removeOwnFile(@NotNull final String ownClusterId) {
        final String objectKey = s3Client.getS3Config().getFilePrefix() + ownClusterId;

        s3Client.deleteObject(objectKey);
        ownNodeFile = null;

        logger.debug("Removed own S3 file '{}'.", objectKey);
    }

    @NotNull
    private List<ClusterNodeAddress> getNodeAddresses() {
        final List<ClusterNodeAddress> nodeAddresses = new ArrayList<>();

        final List<ClusterNodeFile> nodeFiles;
        try {
            nodeFiles = getNodeFiles();
        } catch (final Exception ex) {
            logger.error("Unknown error while reading all node files.", ex);
            return nodeAddresses;
        }

        for (final ClusterNodeFile nodeFile : nodeFiles) {

            if (nodeFile.isExpired(s3Client.getS3Config().getFileExpirationInSeconds())) {

                logger.debug("S3 file of node with clusterId {} is expired. File will be deleted.", nodeFile.getClusterId());

                final String objectKey = s3Client.getS3Config().getFilePrefix() + nodeFile.getClusterId();
                s3Client.deleteObject(objectKey);
            } else {
                nodeAddresses.add(nodeFile.getClusterNodeAddress());
            }
        }

        logger.debug("Found following node addresses with the S3 extension: {}", nodeAddresses);

        return nodeAddresses;
    }

    @NotNull
    private List<ClusterNodeFile> getNodeFiles() {
        final List<ClusterNodeFile> clusterNodeFiles = new ArrayList<>();

        ObjectListing objectListing = s3Client.getObjects(s3Client.getS3Config().getFilePrefix());

        while (objectListing != null) {

            for (final S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {

                if (objectSummary == null) {
                    continue;
                }

                try {
                    final ClusterNodeFile nodeFile = getNodeFile(objectSummary);
                    if (nodeFile != null) {
                        clusterNodeFiles.add(nodeFile);
                    }
                } catch (final AmazonS3Exception ex) {
                    logger.error("Not able to read file {} from bucket {}. Skipping file.", objectSummary.getKey(), s3Client.getS3Config().getBucketName(), ex);
                } catch (final Exception ex) {
                    logger.error("Unknown error occurred while reading file {} from bucket {}. Skipping file.", objectSummary.getKey(), s3Client.getS3Config().getBucketName(), ex);
                }
            }

            if (objectListing.isTruncated()) {
                logger.debug("ObjectListing is truncated. Next batch will be loaded.");
                objectListing = s3Client.getNextBatchOfObjects(objectListing);
            } else {
                objectListing = null;
            }
        }

        return clusterNodeFiles;
    }

    @Nullable
    private ClusterNodeFile getNodeFile(@NotNull final S3ObjectSummary objectSummary) {
        final String objectKey = objectSummary.getKey();
        final S3Object s3Object = s3Client.getObject(objectKey);

        final String fileContent;

        try (final S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
            fileContent = new BufferedReader(new InputStreamReader(s3ObjectInputStream)).readLine();
        } catch (final IOException ex) {
            logger.error("An error occurred while reading the S3 object from an input stream.", ex);
            return null;
        }
        if (fileContent == null || fileContent.isBlank()) {
            logger.debug("S3 object '{}' has no content. Skipping file.", objectKey);
            return null;
        }
        final ClusterNodeFile nodeFile = ClusterNodeFile.parseClusterNodeFile(fileContent);
        if (nodeFile == null) {
            logger.debug("Content of the S3 object '{}' could not parsed. Skipping file.", objectKey);
            return null;
        }
        return nodeFile;
    }
}
