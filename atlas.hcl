env "dev" {
     migration {
        // URL where the migration directory resides.
        dir = "file://migrations"
    }
    format {
        migrate {
            diff = "{{ sql . \"  \" }}"
        }
    }
    //to = "file://schema.sql"
    url = "postgresql://postgres:postgres@localhost:5454/bulkydb?sslmode=disable"
    dev = "docker://postgres/15/dev"
    
}
