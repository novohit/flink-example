package com.wyu.transformation.model;

import lombok.*;

/**
 * @author novo
 * @since 2023-03-28
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Log {

    private Long time;

    private String domain;

    private Double traffic;
}
