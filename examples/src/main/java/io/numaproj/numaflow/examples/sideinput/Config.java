package io.numaproj.numaflow.examples.sideinput;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * A simple config class to hold the source and sampling rate.
 * This config will be serialized and used as a side input message.
 */
@Getter
@Setter
@AllArgsConstructor
@ToString
public class Config {
    private String source;
    private float sampling;
}
