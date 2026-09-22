package com.example.producer.controllers;

import com.example.producer.requests.ProductRequest;
import com.example.producer.services.ProductServiceSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.example.producer.responses.ErrorMessage;

import java.util.Date;

@RestController
@RequestMapping("/products")
public class ProductController {
    private final ProductServiceSync productServiceAsync;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public ProductController(ProductServiceSync productServiceAsync) {
        this.productServiceAsync = productServiceAsync;
    }

    @PostMapping
    public ResponseEntity<Object> createProduct(@RequestBody ProductRequest productRequest) {
        String productId;

        try {
            productId = this.productServiceAsync.createProduct(productRequest);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorMessage(new Date(), e.getMessage(), "/products"));
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
