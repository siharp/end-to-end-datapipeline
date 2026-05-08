# 🚀 Getting Started

## 1. Start the Containers

Run the following command in your terminal to start MinIO in detached mode:

```bash
docker compose up -d
```

---

## 2. Access the Web Console

Once the containers are running, you can access the MinIO Web User Interface (UI):

- **URL:** http://localhost:9001
- **API Port:** `9000`
- **Console Port:** `9001`

---

## 3. Login Credentials

Use the default root credentials to log in:

| Username     | Password     |
|---------------|--------------|
| `minioadmin` | `minioadmin` |

---

# 🛠 Using MinIO Client (`mc`) CLI

If you need to perform manual operations via the command line, you can enter the MinIO Client container.

---

## 1. Enter the Container

Replace `<mc-containername>` with the actual name of your `mc` service container  
(usually `mc-init` or similar):

```bash
docker exec -it <mc-containername> bash
```

---

## 2. Register the MinIO Server (Alias)

Inside the container, link the `mc` tool to your MinIO server.

Use the internal container name for the URL:

```bash
mc alias set myminio http://<miniocontainername>:9000 minioadmin minioadmin
```

---

## 3. Basic Commands

### View Help

Check all available commands and syntax:

```bash
mc --help
```

---

### List Buckets

Verify your connection by listing all existing buckets:

```bash
mc ls myminio
```

---

### Create Bucket (Example)

```bash
mc mb --ignore-existing myminio/my-data
```

---
### Remove Bucket (Example)

```bash
mc rb myminio/your-bucket-name
```