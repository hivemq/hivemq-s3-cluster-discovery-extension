/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.cluster.discovery.s3.callbacks;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.services.cluster.ClusterDiscoveryCallback;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.extensions.cluster.discovery.s3.aws.S3BucketResponse;
import com.hivemq.extensions.cluster.discovery.s3.aws.HiveMQS3Client;
import com.hivemq.extensions.cluster.discovery.s3.config.ClusterNodeFile;
import com.hivemq.extensions.cluster.discovery.s3.config.ConfigurationReader;
import com.hivemq.extensions.cluster.discovery.s3.metrics.ExtensionMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_NAME;
import static com.hivemq.extensions.cluster.discovery.s3.util.StringUtil.isNullOrBlank;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryCallback implements ClusterDiscoveryCallback {

    private static final @NotNull Logger LOG = LoggerFactory.getLogger(S3DiscoveryCallback.class);

    private final @NotNull HiveMQS3Client hiveMQS3Client;
    private final @NotNull ExtensionMetrics extensionMetrics;
    private final @NotNull AtomicInteger addressesCount = new AtomicInteger(0);

    private @Nullable ClusterNodeFile ownNodeFile;

    public S3DiscoveryCallback(
            final @NotNull ConfigurationReader configurationReader, final @NotNull ExtensionMetrics extensionMetrics) {
        this.hiveMQS3Client = new HiveMQS3Client(configurationReader);
        this.extensionMetrics = extensionMetrics;
        extensionMetrics.registerAddressCountGauge(addressesCount::get);
    }

    S3DiscoveryCallback(
            final @NotNull HiveMQS3Client hiveMQS3Client, final @NotNull ExtensionMetrics extensionMetrics) {
        this.hiveMQS3Client = hiveMQS3Client;
        this.extensionMetrics = extensionMetrics;
        extensionMetrics.registerAddressCountGauge(addressesCount::get);
    }

    @Override
    public void init(
            final @NotNull ClusterDiscoveryInput clusterDiscoveryInput,
            final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            hiveMQS3Client.createOrUpdate();
        } catch (final Exception ignored) {
            LOG.error("{}: Configuration couldn't be loaded. Skipping initial discovery.", EXTENSION_NAME);
            extensionMetrics.getResolutionRequestFailedCounter().inc();
            addressesCount.set(0);
            return;
        }
        try {
            final S3BucketResponse s3Bucket = hiveMQS3Client.checkBucket();
            if (s3Bucket.isSuccessful()) {
                saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
                clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());
            } else {
                final S3BucketResponse.Status status = s3Bucket.getStatus();
                if (status == S3BucketResponse.Status.NOT_EXISTING) {
                    LOG.error("{}: Configured bucket '{}' doesn't exist. Skipping initial discovery.",
                            EXTENSION_NAME,
                            s3Bucket.getBucketName());
                } else if (status == S3BucketResponse.Status.NO_PERMISSION) {
                    LOG.error(
                            "{}: No permission for configured bucket '{}'. Please check your credentials and AWS security settings. Skipping initial discovery.",
                            EXTENSION_NAME,
                            s3Bucket.getBucketName());
                } else if (status == S3BucketResponse.Status.OTHER) {
                    LOG.error(
                            "{}: Unknown error occurred when checking configured bucket '{}'. Please check your s3-bucket-region setting. Skipping initial discovery.",
                            EXTENSION_NAME,
                            s3Bucket.getBucketName());
                }
                s3Bucket.getThrowable().ifPresent(throwable -> LOG.debug("{}: Original Exception: ", EXTENSION_NAME, throwable));
                extensionMetrics.getResolutionRequestFailedCounter().inc();
                addressesCount.set(0);
            }
        } catch (final Exception e) {
            LOG.error("{}: Initialization of the S3 discovery callback failed.", EXTENSION_NAME, e);
            extensionMetrics.getResolutionRequestFailedCounter().inc();
            addressesCount.set(0);
        }
    }

    @Override
    public void reload(
            final @NotNull ClusterDiscoveryInput clusterDiscoveryInput,
            final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        try {
            hiveMQS3Client.createOrUpdate();
        } catch (final Exception ignored) {
            LOG.error("{}: Configuration couldn't be reloaded. Skipping reload callback.", EXTENSION_NAME);
            extensionMetrics.getResolutionRequestFailedCounter().inc();
            addressesCount.set(0);
            return;
        }
        try {
            final S3BucketResponse s3Bucket = hiveMQS3Client.checkBucket();
            if (s3Bucket.isSuccessful()) {
                if (ownNodeFile == null ||
                        ownNodeFile.isExpired(Objects.requireNonNull(hiveMQS3Client.getS3Config())
                                .getFileUpdateIntervalInSeconds())) {
                    saveOwnFile(clusterDiscoveryInput.getOwnClusterId(), clusterDiscoveryInput.getOwnAddress());
                }
                clusterDiscoveryOutput.provideCurrentNodes(getNodeAddresses());
            } else {
                final S3BucketResponse.Status status = s3Bucket.getStatus();
                if (status == S3BucketResponse.Status.NOT_EXISTING) {
                    LOG.error("{}: Configured bucket '{}' doesn't exist. Skipping discovery reload callback.",
                            EXTENSION_NAME,
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName());
                } else if (status == S3BucketResponse.Status.NO_PERMISSION) {
                    LOG.error(
                            "{}: No permission for configured bucket '{}'. Please check your credentials and AWS security settings. Skipping discovery reload callback.",
                            EXTENSION_NAME,
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName());
                } else if (status == S3BucketResponse.Status.OTHER) {
                    LOG.error(
                            "{}: Unknown error occurred when checking configured bucket '{}'. Please check your s3-bucket-region setting. Skipping discovery reload callback.",
                            EXTENSION_NAME,
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName());
                }
                s3Bucket.getThrowable().ifPresent(throwable -> LOG.debug("{}: Original Exception: ", EXTENSION_NAME, throwable));
                extensionMetrics.getResolutionRequestFailedCounter().inc();
                addressesCount.set(0);
            }
        } catch (final Exception e) {
            LOG.error("{}: Reload of the S3 discovery callback failed.", EXTENSION_NAME, e);
            extensionMetrics.getResolutionRequestFailedCounter().inc();
            addressesCount.set(0);
        }
    }

    @Override
    public void destroy(final @NotNull ClusterDiscoveryInput clusterDiscoveryInput) {
        try {
            if (ownNodeFile != null) {
                deleteOwnFile(clusterDiscoveryInput.getOwnClusterId());
            }
        } catch (final Exception e) {
            LOG.error("{}: Destroy of the S3 discovery callback failed.", EXTENSION_NAME, e);
        }
    }

    private void saveOwnFile(
            final @NotNull String ownClusterId, final @NotNull ClusterNodeAddress ownAddress) {
        final String objectKey = Objects.requireNonNull(hiveMQS3Client.getS3Config()).getFilePrefix() + ownClusterId;
        final ClusterNodeFile newNodeFile = new ClusterNodeFile(ownClusterId, ownAddress);

        hiveMQS3Client.saveObject(objectKey, newNodeFile.toString());
        ownNodeFile = newNodeFile;

        LOG.debug("{}: Updated own S3 file '{}'.", EXTENSION_NAME, objectKey);
    }

    private void deleteOwnFile(final @NotNull String ownClusterId) {
        final String objectKey = Objects.requireNonNull(hiveMQS3Client.getS3Config()).getFilePrefix() + ownClusterId;

        hiveMQS3Client.deleteObject(objectKey);
        ownNodeFile = null;

        LOG.debug("{}: Removed own S3 file '{}'.", EXTENSION_NAME, objectKey);
    }

    private @NotNull List<ClusterNodeAddress> getNodeAddresses() {
        final List<ClusterNodeAddress> nodeAddresses = new ArrayList<>();

        final List<ClusterNodeFile> nodeFiles;
        try {
            nodeFiles = getNodeFiles();
        } catch (final Exception e) {
            LOG.error("{}: Unknown error while reading all node files.", EXTENSION_NAME, e);
            extensionMetrics.getResolutionRequestFailedCounter().inc();
            addressesCount.set(0);
            return nodeAddresses;
        }

        for (final ClusterNodeFile nodeFile : nodeFiles) {
            if (nodeFile.isExpired(Objects.requireNonNull(hiveMQS3Client.getS3Config()).getFileExpirationInSeconds())) {
                LOG.debug("{}: S3 file of node with clusterId {} is expired. File will be deleted.",
                        EXTENSION_NAME,
                        nodeFile.getClusterId());

                final String objectKey = hiveMQS3Client.getS3Config().getFilePrefix() + nodeFile.getClusterId();
                hiveMQS3Client.deleteObject(objectKey);
            } else {
                nodeAddresses.add(nodeFile.getClusterNodeAddress());
            }
        }
        LOG.debug("{}: Found following node addresses: {}", EXTENSION_NAME, nodeAddresses);
        extensionMetrics.getResolutionRequestCounter().inc();
        addressesCount.set(nodeAddresses.size());
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
                    LOG.error("{}: Not able to read file {} from bucket {}. Skipping file.",
                            EXTENSION_NAME,
                            s3Object.key(),
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName(),
                            e);
                } catch (final Exception e) {
                    LOG.error("{}: Unknown error occurred while reading file {} from bucket {}. Skipping file.",
                            EXTENSION_NAME,
                            s3Object.key(),
                            Objects.requireNonNull(hiveMQS3Client.getS3Config()).getBucketName(),
                            e);
                }
            }

            if (listObjectsV2Response.isTruncated()) {
                LOG.debug("{}: ObjectListing is truncated. Next batch will be loaded.", EXTENSION_NAME);
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
            LOG.error("{}: An error occurred while reading the S3 object from an input stream.", EXTENSION_NAME, e);
            return null;
        }
        if (isNullOrBlank(fileContent)) {
            LOG.debug("{}: S3 object '{}' has no content. Skipping file.", EXTENSION_NAME, objectKey);
            return null;
        }
        final ClusterNodeFile nodeFile = ClusterNodeFile.parseClusterNodeFile(fileContent);
        if (nodeFile == null) {
            LOG.debug("{}: Content of the S3 object '{}' could not parsed. Skipping file.", EXTENSION_NAME, objectKey);
            return null;
        }
        return nodeFile;
    }
}
