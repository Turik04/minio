package com.example.miniodb.service;

import com.example.miniodb.model.FileEntity;
import com.example.miniodb.repository.FileRepository;
import io.minio.*;
import io.minio.messages.Item;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class MinioService {

    private final MinioClient minioClient;
    private final FileRepository fileRepository;

    @Value("${minio.bucket-name}")
    private String bucketName;

    public void createBucketIfNotExists() {
        try {
            boolean exists = minioClient.bucketExists(
                    BucketExistsArgs.builder().bucket(bucketName).build()
            );
            if (!exists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                System.out.println("Bucket '" + bucketName + "' создан");
            } else {
                System.out.println("Bucket '" + bucketName + "' уже существует");
            }
        } catch (Exception e) {
            throw new RuntimeException("Ошибка при создании bucket: " + e.getMessage(), e);
        }
    }

    public void uploadFile(MultipartFile file) {
        try {
            createBucketIfNotExists();

            String objectName = file.getOriginalFilename();

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .stream(file.getInputStream(), file.getSize(), -1)
                            .contentType(file.getContentType())
                            .build()
            );

            saveFileToDB(objectName, file);
            System.out.println("Файл загружен: " + objectName);

        } catch (Exception e) {
            throw new RuntimeException("Ошибка при загрузке файла: " + e.getMessage(), e);
        }
    }

    public String uploadMultipleFiles(MultipartFile[] files, String folderName) {
        try {
            createBucketIfNotExists();

            String finalFolder = (folderName != null && !folderName.isEmpty())
                    ? folderName
                    : "upload_";

            for (MultipartFile file : files) {
                String objectName = finalFolder + "/" + file.getOriginalFilename();

                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .stream(file.getInputStream(), file.getSize(), -1)
                                .contentType(file.getContentType())
                                .build()
                );

                saveFileToDB(objectName, file);
                System.out.println("Загружен файл: " + objectName);
            }

            return finalFolder;

        } catch (Exception e) {
            throw new RuntimeException("Ошибка загрузки файлов: " + e.getMessage(), e);
        }
    }

    private void saveFileToDB(String filePath, MultipartFile file) {
        String original = file.getOriginalFilename();
        String ext = (original != null && original.contains("."))
                ? original.substring(original.lastIndexOf(".") + 1)
                : "";

        FileEntity entity = FileEntity.builder()
                .fileName(filePath)
                .originalName(original)
                .extension(ext)
                .size(file.getSize())
                .contentType(file.getContentType())
                .uploadDate(LocalDateTime.now())
                .build();

        fileRepository.save(entity);
        System.out.println("Сохранено в БД: " + filePath);
    }

    public InputStream downloadFile(String fileName) {
        try {
            return minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(fileName)
                            .build()
            );
        } catch (Exception e) {
            throw new RuntimeException("Ошибка при скачивании файла: " + e.getMessage(), e);
        }
    }

    public String deleteFileById(Long id) {
        try {
            FileEntity fileEntity = fileRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("Файл с ID " + id + " не найден."));

            String filePath = fileEntity.getFileName();

            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucketName)
                            .object(filePath)
                            .build()
            );
            System.out.println("Файл удалён из MinIO: " + filePath);

            fileRepository.delete(fileEntity);
            System.out.println("Файл с ID " + id + " удалён из базы данных.");

            Iterable<Result<Item>> remainingFiles = minioClient.listObjects(
                    ListObjectsArgs.builder().bucket(bucketName).build()
            );

            boolean isEmpty = !remainingFiles.iterator().hasNext();
            if (isEmpty) {
                minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
                System.out.println("Bucket '" + bucketName + "' пуст и был удалён.");
                return "Файл удалён, bucket также удалён (так как был пуст).";
            }

            return "Файл с ID " + id + " успешно удалён.";

        } catch (Exception e) {
            throw new RuntimeException("Ошибка при удалении файла по ID: " + e.getMessage(), e);
        }
    }
}
