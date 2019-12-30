package com.atguigu.gmall0715.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall0715.publisher.mapper")
@ComponentScan(basePackages = "com.atguigu.gmall0715.publisher")
public class Gmall0715PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0715PublisherApplication.class, args);
    }

}
