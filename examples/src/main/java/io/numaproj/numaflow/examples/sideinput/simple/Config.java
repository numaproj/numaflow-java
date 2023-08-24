package io.numaproj.numaflow.examples.sideinput.simple;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Config {
    private String source;
    private float dropRatio;
}
