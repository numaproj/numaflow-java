package io.numaproj.numaflow.examples.sideinput.simple;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class Config {
    private String source;
    private float sampling;
}
