package io.numaproj.numaflow.function.interfaces;

/**
 * DatumMetadata contains methods to get the metadata information for datum.
 */
public interface DatumMetadata {
    /**
     * method to get the ID of the Datum
     * @return returns the ID of the Datum.
     */
    public String getId();

    /**
     * method to get the numDelivered value.
     * @return returns the number of times the datum has been delivered.
     */
    public long getNumDelivered();
}
