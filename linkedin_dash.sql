   --requete pour créer une BD --
    create or replace database linkedin;

      --requete pour créer un Schema --
    create or replace schema raw;

    -- Create external S3 stage 
    create stage linkedin_csv  URL =' s3://snowflake-lab-bucket/'; 

    --vérifier si tous les fichiers sont bien importés --
    list @linkedin_csv;

    --créer un file format pour les fichiers csv--
    CREATE or replace FILE FORMAT csv 
    TYPE = 'CSV' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 1
    field_optionally_enclosed_by = '\042'
    null_if = ('');

   --créer un file format pour les fichiers JSON--
    CREATE or REPLACE file format json 
    type = 'JSON'
    STRIP_OUTER_ARRAY=TRUE;
    
    --création de table job_postings--
    CREATE OR REPLACE TABLE raw.job_postings (
      job_id STRING PRIMARY KEY,
      company_id STRING,  -- <- clé étrangère
      company_name STRING,
      title STRING,
      description STRING,
      max_salary FLOAT,
      pay_period STRING,
      formatted_work_type STRING,
      location STRING,
      applies STRING,
      original_listed_time STRING,
      remote_allowed FLOAT,
      views INT,
      job_posting_url STRING,
      application_url STRING,
      application_type STRING,
      expiry STRING,
      closed_time STRING,
      formatted_experience_level STRING,
      skills_desc STRING,
      listed_time STRING,
      posting_domain STRING,
      sponsored BOOLEAN,
      work_type STRING,
      currency STRING,
      compensation_type STRING,
      FOREIGN KEY (company_id) REFERENCES raw.companies(company_id)    
      );

    --création de table Benefits--
    CREATE OR REPLACE TABLE raw.benefits  (
      job_id STRING,
      inferred BOOLEAN,
      type STRING,
      FOREIGN KEY (job_id) REFERENCES raw.job_postings(job_id)
    );

    --création de table companies--
    CREATE OR REPLACE TABLE raw.companies (
        company_id STRING PRIMARY KEY,
        name STRING,
        description STRING,
        company_size STRING,
        state STRING,
        country STRING,
        city STRING,
        zip_code STRING,
        address STRING,
        url STRING
    );
    
    --création de table company_industries--
    CREATE OR REPLACE TABLE raw.company_industries (
        company_id STRING,
        industry STRING,
        FOREIGN KEY (company_id) REFERENCES raw.companies(company_id)
    );
    
    --création de table company_specialities--
    CREATE OR REPLACE TABLE raw.company_specialities (
        company_id STRING,
        speciality STRING,
        FOREIGN KEY (company_id) REFERENCES raw.companies(company_id)
    );

    --création de table employee_counts--
    CREATE OR REPLACE TABLE raw.employee_counts (
        company_id STRING,
        employee_count NUMBER,
        follower_count NUMBER,
        time_recorded STRING,
        FOREIGN KEY (company_id) REFERENCES raw.companies(company_id)
    );

    --création de table job_industries--
    CREATE OR REPLACE TABLE raw.job_industries (
        job_id STRING,
        industry_id STRING,
        FOREIGN KEY (job_id) REFERENCES raw.job_postings(job_id)
    );
    
    --cration de table job_skills--
   CREATE OR REPLACE TABLE raw.job_skills (
        job_id STRING,
        skill_abr STRING,
        FOREIGN KEY (job_id) REFERENCES raw.job_postings(job_id)
    );
    --créer une table industries qui mappe industry_id à un nom.
    CREATE OR REPLACE TABLE raw.industries (
    industry_id STRING PRIMARY KEY,
    industry_name STRING
);

      --charger les données  dans la table job_postings --
      COPY INTO raw.job_postings
    FROM @linkedin_csv/job_postings.csv
    FILE_FORMAT = (
      TYPE = 'CSV',
      SKIP_HEADER = 0,
      FIELD_OPTIONALLY_ENCLOSED_BY = '"',
      PARSE_HEADER = TRUE,
      ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    )
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    
     --vérifier le chargement des données--
    SELECT * FROM raw.job_postings;

    --charger les données  dans la table benefits --
    COPY INTO raw.benefits FROM @linkedin_csv/benefits.csv
    FILE_FORMAT =csv PATTERN = '.*csv.*';
    
    --vérifier le chargement des données--
    select * from raw.benefits;
    

     --charger les données  dans la table companies --
    COPY INTO raw.companies FROM @linkedin_csv/companies.json 
    file_format =json MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

    --vérifier le chargement des données--
    select * from raw.companies;

    --charger les données  dans la table company_industries --
    COPY INTO raw.company_industries FROM @linkedin_csv/company_industries.json 
    file_format =json MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        
    --vérifier le chargement des données--
    select * from raw.company_industries;

    --charger les données  dans la table company_specialities --
    COPY INTO raw.company_specialities FROM @linkedin_csv/company_specialities.json 
    file_format =json MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        
    --vérifier le chargement des données--
    select * from raw.company_specialities;

     --charger les données  dans la table employee_counts --
    COPY INTO raw.employee_counts FROM @linkedin_csv/employee_counts
    FILE_FORMAT =csv PATTERN = '.*csv.*';
    
    --vérifier le chargement des données--
    select * from raw.employee_counts;
    
    --charger les données  dans la table job_industries --
    COPY INTO raw.job_industries FROM @linkedin_csv/job_industries.json 
    file_format= json MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    
    --vérifier le chargement des données--
    select * from raw.job_industries;
    
    --charger les données  dans la table job_skills --
    COPY INTO raw.job_skills
    FROM @linkedin_csv/job_skills.csv
    FILE_FORMAT =csv ON_ERROR = 'CONTINUE' PATTERN = '.*csv.*';

    --vérifier le chargement des données--
    select * from raw.job_skills;
    
    --Requete1: top 10 des titres de postes les plus publiés par industrie.--
     SELECT 
        ji.industry_id,
        jp.title,
        COUNT(*) AS post_count
    FROM raw.job_postings jp
    JOIN raw.job_industries ji ON jp.job_id = ji.job_id
    GROUP BY ji.industry_id, jp.title
    ORDER BY post_count DESC
    LIMIT 10;

--Requete2:Top 10 des postes les mieux rémunérés par industrie.--
    SELECT 
        ji.industry_id,
        jp.title,
        MAX(jp.max_salary) AS max_salary
    FROM raw.job_postings jp
    JOIN raw.job_industries ji ON jp.job_id = ji.job_id
    WHERE jp.max_salary IS NOT NULL
    GROUP BY ji.industry_id, jp.title
    ORDER BY max_salary DESC
    LIMIT 10;

--requete3:Répartition des offres d’emploi par taille d’entreprise.
        SELECT 
        c.name AS company_name,
        c.company_size,
        COUNT(*) AS nb_offres
    FROM raw.job_postings jp
    JOIN raw.companies c 
        ON SPLIT_PART(jp.company_id, '.', 1) = c.company_id
    WHERE c.company_size IS NOT NULL
    GROUP BY c.name, c.company_size
    ORDER BY nb_offres DESC;


     --requete4:Répartition des offres d’emploi par secteur d’activité.
              SELECT 
        ji.industry_id AS industry,
        COUNT(*) AS nb_offres
    FROM raw.job_postings jp
    JOIN raw.job_industries ji 
        ON jp.job_id = ji.job_id
    WHERE ji.industry_id IS NOT NULL
    GROUP BY ji.industry_id
    ORDER BY nb_offres DESC;

     --requete5:Répartition des offres d’emploi par type d’emploi (temps plein, stage, temps partiel). 
        SELECT work_type, COUNT(*) AS nb_offres
    FROM raw.job_postings
    WHERE work_type IS NOT NULL
    GROUP BY work_type;



   





    