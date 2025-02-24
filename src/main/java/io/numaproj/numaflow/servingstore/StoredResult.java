package io.numaproj.numaflow.servingstore;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * StoredResult is the data stored in the store per origin.
 */
@Getter
@Builder
@AllArgsConstructor
public class StoredResult {
    // ID is the unique identifier for the StoredResult.
    private String id;
    // Payloads is the list of Payloads stored in the Store for the given ID.
    private List<Payload> payloads;
}
