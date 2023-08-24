package io.numaproj.numaflow.examples.sideinput.simple;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Config {
    private String source;
    private float dropRatio;
}
