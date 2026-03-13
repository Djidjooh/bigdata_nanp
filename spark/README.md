# 🦞 bigdata_nanp - SPARK (PySpark) — BigData Processing stack

Tools (`Python`) used to process `.parquet` files store in a `minio` bucket.

First of all we consider, you're able to ingest (`streaming`, `batch`) data from  majors sources (`Mysql`, `files`. ...), an store them in `Minio`.

For example purpose, your datas (`client`, `product` and `sales`) are stored in buckets `raw-bucket/[client|product|sales]/*.parquet`.



<p align="center">
    <picture>
        <source media="(prefers-color-scheme: light dark)" srcset="images/archi.drawio.png">
        <img src="images/archi.drawio.png" alt="BigData streaming stack (docker)" width="600" height="300">
    </picture>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge" alt="MIT License"></a>
</p>

---

**Stack** is a simple *BigData Processing stack* running over *Docker*.

It will help you to deploy and test a simple **Processing** pipeline using **Docker** and **Spark(pyspark)**.

---

**Note: the most usefull are `minio, spark-master, jupyter-pyspark`.**

## **Components:**

- **minio:** source & store result in `parquet` format. It use bucket `raw-redpanda` for source and `refine-bucket` for dastination.
- **spark cluster:** Spread over three nodes (`spark-master`, `spark-worker1`, `spark-worker2`). used to process datas in `raw-bucket` ( `client`, `product`, `sales`). `saprk-master` act as `master node` and the two others as `slave or workers nodes`. **NOTES:** If it's take too much resources, comment or shutdown `spark-worker1` or `spark-worker2` or also transform `spark-master` as **master** and **worker** at the same time.
- **jupyter-pyspark:** Notebook, connect to `spark cluster` and used to run `pyspark` scripts.

---

## **Files & Folders:**

1- **images:** contains screenshot.

2- **notes:** some `python scripts`:

- `notes_parquet.py`: read data in minio (and store them in another bucket)

- `notes_iceberg.py`: use `spark iceberg catalog` to store in `refine-bucket`.

- `job.py`: will help you to run a simple `pyspark` script in command line.

3- **notebooks:** some `notebooks`:

- `first_parquet.ipynb`: read data in minio (and store them in another bucket)

- `iceberg_parquet.ipynb`: use `spark iceberg catalog` to store in `refine-bucket`.

4- **datas:** contains 3 files, but only 2 are usefull.

- `client_bucket_topics.zip`: datas to upload in bucket `client-bucket`.

- `raw_bucket_client_product_sales_parquet.zip`: datas to upload in bucket `raw-bucket`.

---

## **PORTS & configs**

- **Minio UI**: Default -> `9001`, Exposed -> `9031`. **`[http://localhost:9031]`**

- **Minio API S3**: Default -> `9000`, Exposed -> `9030`

- **spark-master**: Default (http) -> `8080`, Exposed(http) -> `8980`. **`[http://localhost:8980]`**

- **Jupyter Notebooks**: Default -> `8888`, Exposed -> `8988`. **`[http://localhost:8988]`**

---

### **Volumes**
before you start the docker stack, make sure to change volumes locations

```yml

volumes:
  pyspark_data:
    driver: local # Define the driver and options under the volume name
    driver_opts:
      type: none
      device: /Change/Path/pyspark
      o: bind
  jupyter_pyspark_data:
    driver: local # Define the driver and options under the volume name
    driver_opts:
      type: none
      device: /Change/Path/jupyter-pyspark
      o: bind
  minio_data:
    driver: local # Define the driver and options under the volume name
    driver_opts:
      type: none
      device: /Change/Path/minio
      o: bind
  share_data:
    driver: local # Define the driver and options under the volume name
    driver_opts:
      type: none
      device: /Change/Path/share_folder
      o: bind
```

---

### **Configs & setup**

* #### **Step 1:** Create all volumes folders inside your main host (see volumes above)

* #### **step 2:** Clone the repo and move to folder **`saprk`**

* #### **step 3:** Change volumes path inside **`compose.yml`** file under section **`volumes:`** (see above)

* #### **step 4:** Start your project inside **`compose.yml`**.

```sh
# Be sure to be in the folder with compose.yml file
# start all
docker compose up -d
```

* #### **step 5:** if not yet, create bucket **`raw-bucket`**.

* #### **step 6:** unzip files **`datas/raw_bucket_client_product_sales_parquet.zip`** and import folders **`client, product and sales`** inside bucket **`raw-bucket`**.

* #### **step 7:** also import notebooks **`notebooks/*.ipynb`** inside **`work`** folder in jupyter.


---

### **Run project & some cleaning ops**

```sh
# Be sure to be in the folder with compose.yml file
# start all
docker compose up -d

# stop all and clean some volume
docker compose down -v --remove-orphans
```

---

### **Troubleshooting**

* #### **delete minio bucket:** you can use **minio UI** container to recreate all bucket.

```sh
# delete a bucket
docker exec minio mc rb --force myalias/[nom-du-bucket]
```

---

### **Some commands**

---

## **Project**

- ### **minio**

url: **`http://localhost:9031/`**

create buckets `raw-bucket` and `refine-bucket`


* ### **Container:** Deployed containers

<p align="center">
    <picture>
        <source media="(prefers-color-scheme: light dark)" srcset="images/portainer.png">
        <img src="images/portainer.png" alt="Containers list" width="600" height="400">
    </picture>
  </p>

* ### **spark Cluster**

  * **console**: **`https://localhost:8100/`** 

  <p align="center">
    <picture>
        <source media="(prefers-color-scheme: light dark)" srcset="images/spark-cluster.png">
        <img src="images/spark-cluster.png" alt="Redpanda console Cluster Details" width="600" height="400">
    </picture>
  </p>

* ### **Minio:** Parquet files

<p align="center">
    <picture>
        <source media="(prefers-color-scheme: light dark)" srcset="images/rw-minio-data.png">
        <img src="images/rw-minio-data.png" alt="Containers list" width="600" height="400">
    </picture>
</p>

* ### **Notebook - client & Pyspark:** 

<p align="center">
    <picture>
        <source media="(prefers-color-scheme: light dark)" srcset="images/jupyter1.png">
        <img src="images/jupyter1.png" alt="Containers list" width="600" height="300">
    </picture>
</p>


Enjoy!


