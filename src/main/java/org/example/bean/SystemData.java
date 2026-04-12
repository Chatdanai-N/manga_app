package org.example.bean;

import lombok.Data;

import java.util.List;

@Data
public class SystemData {

    private String protagonist;
    private String rank_category;

    private String className;
    private List<String> iconicSummon;
    private List<String> signatureSkill;
}
