package com.example.producer.controllers;

import com.example.producer.requests.ProductRequest;
import com.example.producer.services.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/products")
public class ProductController {
    ProductService productService;

    @PostMapping
    public ResponseEntity<String> createProduct(@RequestBody ProductRequest productRequest) {
        String productId = this.productService.createProduct(productRequest);

        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
