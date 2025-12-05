package com.example.miniodb.controller;

import com.example.miniodb.service.MinioService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/files")
@RequiredArgsConstructor
public class FileController {

    private final MinioService minioService;


    @PostMapping("/upload/png")
    public ResponseEntity<String> uploadPng(@RequestParam("file") MultipartFile file) {
        if (!file.getOriginalFilename().toLowerCase().endsWith(".png")) {
            return ResponseEntity.badRequest().body("Ошибка: файл должен быть PNG (*.png)");
        }
        minioService.uploadFile(file);
        return ResponseEntity.ok("PNG файл загружен: " + file.getOriginalFilename());
    }


}
