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

import static com.hivemq.extensions.util.StringUtil.isNullOrBlank;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryCallback implements ClusterDiscoveryCallback {

    private static final Logger logger = LoggerFactory.getLogger(S3DiscoveryCallback.class);

    @NotNull S3Client s3Client;

    private @Nullable ClusterNodeFile ownNodeFile;

    public S3DiscoveryCallback(final @NotNull ConfigurationReader configurationReader) {
        s3Client = new S3Client(configurationReader);
    }

    @Override
    public void init(final @NotNull ClusterDiscoveryInput clusterDiscoveryInput,
                     final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            s3Client.createOrUpdate();
        } catch (final Exception ignored) {
            logger.error("Configuration of the S3 discovery extension couldn't be loaded. Skipping initial discovery.");
            return;
        }
        try {
            if (!s3Client.existsBucket()) {
                logger.error("Configured bucket '{}' doesn't exist. Skipping initial discovery.", s3Client.getS3Config().getBucketName());
                return;
            }
            saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
            clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());
        } catch (final Exception e) {
            logger.error("Initialization of the S3 discovery callback failed.", e);
        }
    }

    @Override
    public void reload(final @NotNull ClusterDiscoveryInput clusterDiscoveryInput,
                       final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            s3Client.createOrUpdate();
        } catch (final Exception ignored) {
            logger.error("Configuration of the S3 discovery extension couldn't be reloaded. Skipping reload callback.");
            return;
        }
        try {
            if (!s3Client.existsBucket()) {
                logger.error("Configured bucket '{}' doesn't exist. Skipping reload callback.", s3Client.getS3Config().getBucketName());
                return;
            }

            if (ownNodeFile == null || ownNodeFile.isExpired(s3Client.getS3Config().getFileUpdateIntervalInSeconds())) {
                saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
            }

            clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());

        } catch (final Exception e) {
            logger.error("Reload of the S3 discovery callback failed.", e);
        }
    }

    @Override
    public void destroy(final @NotNull ClusterDiscoveryInput clusterDiscoveryInput) {
        try {
            if (ownNodeFile != null) {
                deleteOwnFile(clusterDiscoveryInput.getOwnClusterId());
            }
        } catch (final Exception e) {
            logger.error("Destroy of the S3 discovery callback failed.", e);
        }
    }

    private void saveOwnFile(final @NotNull String ownClusterId,
                             final @NotNull ClusterNodeAddress ownAddress) throws IOException {

        final String objectKey = s3Client.getS3Config().getFilePrefix() + ownClusterId;
        final ClusterNodeFile newNodeFile = new ClusterNodeFile(ownClusterId, ownAddress);

        s3Client.saveObject(objectKey, newNodeFile.toString());
        ownNodeFile = newNodeFile;

        logger.debug("Updated own S3 file '{}'.", objectKey);
    }

    private void deleteOwnFile(final @NotNull String ownClusterId) {

        final String objectKey = s3Client.getS3Config().getFilePrefix() + ownClusterId;

        s3Client.deleteObject(objectKey);
        ownNodeFile = null;

        logger.debug("Removed own S3 file '{}'.", objectKey);
    }

    private @NotNull List<ClusterNodeAddress> getNodeAddresses() {

        final List<ClusterNodeAddress> nodeAddresses = new ArrayList<>();

        final List<ClusterNodeFile> nodeFiles;
        try {
            nodeFiles = getNodeFiles();
        } catch (final Exception e) {
            logger.error("Unknown error while reading all node files.", e);
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

    private @NotNull List<ClusterNodeFile> getNodeFiles() {

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
                } catch (final AmazonS3Exception e) {
                    logger.error("Not able to read file {} from bucket {}. Skipping file.", objectSummary.getKey(), s3Client.getS3Config().getBucketName(), e);
                } catch (final Exception e) {
                    logger.error("Unknown error occurred while reading file {} from bucket {}. Skipping file.", objectSummary.getKey(), s3Client.getS3Config().getBucketName(), e);
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

    private @Nullable ClusterNodeFile getNodeFile(final @NotNull S3ObjectSummary objectSummary) {
        final String objectKey = objectSummary.getKey();
        final S3Object s3Object = s3Client.getObject(objectKey);

        final String fileContent;

        try (final S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
             final InputStreamReader in = new InputStreamReader(s3ObjectInputStream);
             final BufferedReader reader = new BufferedReader(in)) {

            fileContent = reader.readLine();

        } catch (final IOException e) {
            logger.error("An error occurred while reading the S3 object from an input stream.", e);
            return null;
        }
        if (isNullOrBlank(fileContent)) {
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
