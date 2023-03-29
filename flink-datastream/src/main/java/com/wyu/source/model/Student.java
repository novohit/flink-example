package com.wyu.source.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author novo
 * @since 2023-03-28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {

    private Long id;

    private String username;

    private Integer age;
}
