package com.example.refdata.client;

import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.protocol.v1.Codec;

import java.time.Duration;
import java.util.List;

/** Everything {@link RefDataClient.Builder} collects, frozen. Entity types are wire numbers; empty = all. */
record ClientSettings(List<Endpoint> endpoints, String clientName, String clientInstance, List<Integer> entityTypes,
                      List<Codec> acceptedCodecs, int maxFrameBytes, Duration startupTimeout, Duration connectTimeout,
                      Duration readTimeout, Duration backoffInitial, Duration backoffMax, boolean keepAlive,
                      Long expectedDataVersion, Long minDataVersion, ValidationPolicy validationPolicy) {

    boolean unfiltered() { return entityTypes.isEmpty(); }
}
