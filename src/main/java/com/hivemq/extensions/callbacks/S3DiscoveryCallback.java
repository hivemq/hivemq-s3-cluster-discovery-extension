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

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.services.cluster.ClusterDiscoveryCallback;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.extensions.aws.HiveMQS3Client;
import com.hivemq.extensions.config.ClusterNodeFile;
import com.hivemq.extensions.config.ConfigurationReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hivemq.extensions.util.StringUtil.isNullOrBlank;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryCallback implements ClusterDiscoveryCallback {

    private static final @NotNull Logger logger = LoggerFactory.getLogger(S3DiscoveryCallback.class);

    @NotNull HiveMQS3Client hiveMQS3Client;

    private @Nullable ClusterNodeFile ownNodeFile;

    public S3DiscoveryCallback(final @NotNull ConfigurationReader configurationReader) {
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
    }

    @Override
    public void init(
            final @NotNull ClusterDiscoveryInput clusterDiscoveryInput,
            final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            hiveMQS3Client.createOrUpdate();
        } catch (final Exception ignored) {
            logger.error("Configuration of the S3 discovery extension couldn't be loaded. Skipping initial discovery.");
            return;
        }
        try {
            if (!hiveMQS3Client.existsBucket()) {
                logger.error("Configured bucket '{}' doesn't exist. Skipping initial discovery.",
                        Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName());
                return;
            }
            saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
            clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());
        } catch (final Exception e) {
            logger.error("Initialization of the S3 discovery callback failed.", e);
        }
    }

    @Override
    public void reload(
            final @NotNull ClusterDiscoveryInput clusterDiscoveryInput,
            final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            hiveMQS3Client.createOrUpdate();
        } catch (final Exception ignored) {
            logger.error("Configuration of the S3 discovery extension couldn't be reloaded. Skipping reload callback.");
            return;
        }
        try {
            if (!hiveMQS3Client.existsBucket()) {
                logger.error("Configured bucket '{}' doesn't exist. Skipping reload callback.",
                        Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName());
                return;
            }

            if (ownNodeFile == null ||
                    ownNodeFile.isExpired(Objects.requireNonNull(hiveMQS3Client.getS3Config())
                            .getFileUpdateIntervalInSeconds())) {
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

    private void saveOwnFile(
            final @NotNull String ownClusterId, final @NotNull ClusterNodeAddress ownAddress) {
        final String objectKey = Objects.requireNonNull(hiveMQS3Client.getS3Config()).getFilePrefix() + ownClusterId;
        final ClusterNodeFile newNodeFile = new ClusterNodeFile(ownClusterId, ownAddress);

        hiveMQS3Client.saveObject(objectKey, newNodeFile.toString());
        ownNodeFile = newNodeFile;

        logger.debug("Updated own S3 file '{}'.", objectKey);
    }

    private void deleteOwnFile(final @NotNull String ownClusterId) {
        final String objectKey = Objects.requireNonNull(hiveMQS3Client.getS3Config()).getFilePrefix() + ownClusterId;

        hiveMQS3Client.deleteObject(objectKey);
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
            if (nodeFile.isExpired(Objects.requireNonNull(hiveMQS3Client.getS3Config()).getFileExpirationInSeconds())) {
                logger.debug("S3 file of node with clusterId {} is expired. File will be deleted.",
                        nodeFile.getClusterId());

                final String objectKey = hiveMQS3Client.getS3Config().getFilePrefix() + nodeFile.getClusterId();
                hiveMQS3Client.deleteObject(objectKey);
            } else {
                nodeAddresses.add(nodeFile.getClusterNodeAddress());
            }
        }
        logger.debug("Found following node addresses with the S3 extension: {}", nodeAddresses);

        return nodeAddresses;
    }

    private @NotNull List<ClusterNodeFile> getNodeFiles() {
        final List<ClusterNodeFile> clusterNodeFiles = new ArrayList<>();

        ListObjectsV2Response listObjectsV2Response = hiveMQS3Client.getObjects();
        while (listObjectsV2Response != null) {
            for (final S3Object s3Object : listObjectsV2Response.contents()) {
                if (s3Object == null) {
                    continue;
                }

                try {
                    final ClusterNodeFile nodeFile = getNodeFile(s3Object);
                    if (nodeFile != null) {
                        clusterNodeFiles.add(nodeFile);
                    }
                } catch (final S3Exception e) {
                    logger.error("Not able to read file {} from bucket {}. Skipping file.",
                            s3Object.key(),
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName(),
                            e);
                } catch (final Exception e) {
                    logger.error("Unknown error occurred while reading file {} from bucket {}. Skipping file.",
                            s3Object.key(),
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName(),
                            e);
                }
            }

            if (listObjectsV2Response.isTruncated()) {
                logger.debug("ObjectListing is truncated. Next batch will be loaded.");
                listObjectsV2Response =
                        hiveMQS3Client.getNextBatchOfObjects(listObjectsV2Response.nextContinuationToken());
            } else {
                listObjectsV2Response = null;
            }
        }
        return clusterNodeFiles;
    }

    private @Nullable ClusterNodeFile getNodeFile(final @NotNull S3Object s3Object) {
        final String objectKey = s3Object.key();
        final String fileContent;

        try {
            fileContent = hiveMQS3Client.getObject(objectKey);
        } catch (final SdkClientException e) {
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
