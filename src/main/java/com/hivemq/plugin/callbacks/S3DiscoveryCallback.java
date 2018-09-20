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

package com.hivemq.plugin.callbacks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringInputStream;
import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.api.annotations.Nullable;
import com.hivemq.plugin.api.services.cluster.ClusterDiscoveryCallback;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.plugin.configuration.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class S3DiscoveryCallback implements ClusterDiscoveryCallback {

    //FIXME: add logger and logstatements
    //    private static final Logger log = LoggerFactory.getLogger(S3DiscoveryCallback.class);

    private static final @NotNull String SEPARATOR = "||||";
    private static final @NotNull String SEPARATOR_REGEX = "\\|\\|\\|\\|";
    private static final @NotNull String VERSION = "1";

    private final @NotNull AmazonS3 s3;
    private final @NotNull Configuration configuration;
    private final @NotNull String bucketName;
    private @Nullable String objectKey;
    private @Nullable String clusterId;
    private @Nullable ClusterNodeAddress ownAddress;

    public S3DiscoveryCallback(final @NotNull AmazonS3 s3,
                               final Configuration configuration) {
        this.s3 = s3;
        this.s3.setEndpoint(configuration.getEndpoint());
        this.s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(configuration.withPathStyleAccess()));
        this.configuration = configuration;
        this.bucketName = configuration.getBucketName();
    }



    @Override
    public void init(@NotNull final ClusterDiscoveryInput clusterDiscoveryInput, @NotNull final ClusterDiscoveryOutput clusterDiscoveryOutput) {

        try {
            this.clusterId = clusterDiscoveryInput.getOwnClusterId();
            this.ownAddress = clusterDiscoveryInput.getOwnAddress();

            objectKey = configuration.getFilePrefix() + clusterId;

            saveOwnInformationToS3();

            //FIXME: schedule update own information as soon as ManagedPluginExecutor is merged.

            loadAndProvideNodes(clusterDiscoveryOutput);

            clusterDiscoveryOutput.setReloadInterval(30);

        } catch (final Exception e) {
            // log.error("S3 initialization failed");
            // log.debug("Original exception", e);
        }

    }


    @Override
    public void reload(@NotNull final ClusterDiscoveryInput clusterDiscoveryInput, @NotNull final ClusterDiscoveryOutput clusterDiscoveryOutput) {

        try {
            loadAndProvideNodes(clusterDiscoveryOutput);
        } catch (final Exception e) {
        // log.error("Not able to reload nodes from S3");
        // log.debug("Original exception", e);
        }

    }
    @Override
    public void destroy(@NotNull final ClusterDiscoveryInput clusterDiscoveryInput) {
        try {
            s3.deleteObject(bucketName, objectKey);
        } catch (final Exception e) {
//            log.error("Not able to delete object from S3");
//            log.debug("Original exception", e);
        }
    }

    private void loadAndProvideNodes(final @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput) {
        final List<ClusterNodeAddress> addresses = new ArrayList<>();
        final ObjectListing objectListing = s3.listObjects(bucketName, configuration.getFilePrefix());
        readAllFiles(addresses, objectListing);
        clusterDiscoveryOutput.provideCurrentNodes(addresses);
    }

    private void saveOwnInformationToS3() {

        try {
            final String content = createFileContent(clusterId, ownAddress);
            final StringInputStream input;
            input = new StringInputStream(content);
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(input.available());

            s3.putObject(bucketName, objectKey, input, metadata);
//            log.debug("S3 node information updated");

        } catch (final Exception e) {
//            log.error("Not able to save node information to S3");
//            log.debug("Original exception", e);
        }
    }

    private String createFileContent(final @NotNull String clusterId, final @NotNull ClusterNodeAddress ownAddress) {
        final String content = VERSION + SEPARATOR
                + Long.toString(System.currentTimeMillis()) + SEPARATOR
                + clusterId + SEPARATOR
                + ownAddress.getHost() + SEPARATOR
                + ownAddress.getPort() + SEPARATOR;

        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }


    private void readAllFiles(final @NotNull List<ClusterNodeAddress> addresses, @NotNull final ObjectListing objectListing) {
        for (final S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            try {

                final String key = objectSummary.getKey();
                final S3Object object;
                try {
                    object = s3.getObject(bucketName, key);
                } catch (final AmazonS3Exception e) {
//                    log.debug("Not able to read file {} from S3: {}", key, e.getMessage());
                    continue;
                }

                final S3ObjectInputStream objectContent = object.getObjectContent();

                final String fileContent = new BufferedReader(new InputStreamReader(objectContent)).readLine();

                final ClusterNodeAddress address = parseFileContent(fileContent, key);
                if (address != null) {
                    addresses.add(address);
                }

                try {
                    objectContent.close();
                } catch (final IOException e) {
//                    log.trace("Not able to close S3 input stream", e);
                }

                if (objectListing.isTruncated()) {
                    final ObjectListing objectListingNext = s3.listNextBatchOfObjects(objectListing);
                    readAllFiles(addresses, objectListingNext);
                }

            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    private @Nullable ClusterNodeAddress parseFileContent(final @Nullable String fileContent, final @NotNull String key) {

        if (fileContent == null) {
            return null;
        }

        final String content;
        try {
            final byte[] decode = Base64.getDecoder().decode(fileContent);
            if (decode == null) {
//                log.debug("Not able to parse contents from S3-object '{}'", key);
                return null;
            }
            content = new String(decode, StandardCharsets.UTF_8);
        } catch (final IllegalArgumentException e) {
//            log.debug("Not able to parse contents from S3-object '{}'", key);
            return null;
        }

        final String[] split = content.split(SEPARATOR_REGEX);
        if (split.length < 4) {
//            log.debug("Not able to parse contents from S3-object '{}'", key);
            return null;
        }

        final long expirationMinutes = configuration.getExpirationMinutes();

        if (expirationMinutes > 0) {
            final long expirationFromFile = Long.parseLong(split[1]);
            if (expirationFromFile + (expirationMinutes * 60000) < System.currentTimeMillis()) {
//                log.debug("S3 object {} expired, deleting it.", key);
                s3.deleteObject(bucketName, key);
                return null;
            }
        }

        final String host = split[3];
        if (host.length() < 1) {
//            log.debug("Not able to parse contents from S3-object '{}'", key);
            return null;
        }

        final int port;
        try {
            port = Integer.parseInt(split[4]);
        } catch (final NumberFormatException e) {
//            log.debug("Not able to parse contents from S3-object '{}'", key);
            return null;
        }

        return new ClusterNodeAddress(host, port);
    }
}
