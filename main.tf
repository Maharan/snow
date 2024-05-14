terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
}

provider "snowflake" {
  user = "tf-snow"
  role = "SYSADMIN"
  account  = var.snowflake_account
  private_key    = var.snowflake_private_key
  authenticator = "JWT"
}

resource "snowflake_database" "db" {
  name = "TF_DEMO"
}

resource "snowflake_warehouse" "warehouse" {
  name           = "TF_DEMO"
  warehouse_size = "xsmall"
  auto_suspend   = 60
}


resource "snowflake_schema" "RAW" {
  database   = snowflake_database.db.name
  name       = "RAW"
  is_managed = false
}

resource "snowflake_schema" "STAGE" {
  database   = snowflake_database.db.name
  name       = "STAGE"
  is_managed = false
}

resource "snowflake_schema" "MART" {
  database   = snowflake_database.db.name
  name       = "MART"
  is_managed = false
}

# Haven't got this to work out just yet
/* resource "snowflake_table" "table" {
  database                    = "db"
  schema                      = "RAW"
  name                        = "table"

  column {
    name     = "city"
    type     = "text"
  }

  column {
    name     = "year"
    type     = "text"
  }

  column {
    name = "type"
    type = "text"
  }

  column {
    name    = "sport"
    type    = "text"
  }

  column {
    name     = "event"
    type     = "text"
  }

  column {
    name     = "medal"
    type     = "text"
  }

  column {
    name     = "country_code"
    type     = "text"
  }

  column {
    name    = "athlete_full_name"
    type    = "text"
  }
  
  column {
    name    = "time_added"
    type    = "text"
  }
}*/