package com.example.repository.entity;


import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "manga_system_parameter")
public class MangaSystemParameter {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long parameterId;
    @Column(name = "GROUP_CODE",  length = 255,nullable = false)
    private String groupCode;
    @Column(name = "CODE",  length = 255,nullable = false)
    private String code;
    @Column(name = "VALUE", length = 255, nullable = false)
    private String value;
    @Column(name = "ORDER_NUMBER", nullable = false)
    private int orderNumber;



}
