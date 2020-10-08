library(DBI)
library(dplyr)
con <- DBI::dbConnect(odbc::odbc(), .connection_string = "Driver={ODBC Driver 17 for SQL Server};", 
    timeout = 10, server = "18.215.74.4", uid = "judalpalau", 
    pwd = "MovUp2020")

list_databases <- "SELECT name FROM master.dbo.sysdatabases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');"
list_databases <- dbGetQuery(con, list_databases)

query_tables <- "
SELECT 
    t.NAME AS TableName,
    s.Name AS SchemaName,
    p.rows,
    SUM(a.total_pages) * 8 AS TotalSpaceKB, 
    CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB,
    SUM(a.used_pages) * 8 AS UsedSpaceKB, 
    CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS UsedSpaceMB, 
    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB,
    CAST(ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS NUMERIC(36, 2)) AS UnusedSpaceMB
FROM 
    sys.tables t
INNER JOIN      
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255 
GROUP BY 
    t.Name, s.Name, p.Rows
ORDER BY 
    TotalSpaceMB DESC, t.Name
"

for (db in list_databases$name){
  # print(db)
  try({
    dbGetQuery(con, sprintf("USE %s", db));
    assign(db, dbGetQuery(con,query_tables))
  })
}


# EXTRAER DATA DE CCU
library(dbplyr)
dbGetQuery(con, "USE CCU_PORTEO_STAGE")
Track_Viaje_DB <- tbl(con,"Track_Viaje")
Track_Positions_DB <- tbl(con, "Track_Positions")


colnames(Track_Viaje_DB)

range <- Track_Positions_DB %>%  summarise(fechamin = min(Fecha, na.rm = TRUE), 
                              fechamax = max(Fecha, na.rm = TRUE)) %>% as.data.frame()

range <- as.character(seq(range$fechamin, range$fechamax, "hour"))
i <- 1
setwd("~/Analytics Caps/Diplomado Deep Learning/Proyecto/ElasticSearch/tablas")
# (length(range)-1)
for (i in 301:600) {
  range1 <- range[i]; range2 <- range[i+1]
  tabla <- Track_Positions_DB %>% 
    select(Fecha, Patente, Proveedor, Latitud, Longitud, altitude, Direccion, Velocidad, Ignicion, carrier, horometer, panic) %>% 
    filter(Fecha >= range1, Fecha < range2) %>% data.frame()

  write.csv(tabla, sprintf("Track_Positions/Tabla_%s.csv",i), row.names = FALSE)
  rm(tabla)
  gc()
  print(i)
}

colnames(Track_Viaje_DB)

range <- Track_Viaje_DB %>%  summarise(fechamin = min(FechaViaje, na.rm = TRUE), 
                                           fechamax = max(FechaViaje, na.rm = TRUE)) %>% as.data.frame()

range <- as.character(seq(range$fechamin, range$fechamax, "day"))

for (i in 1:(length(range)-1)) {
  range1 <- range[i]; range2 <- range[i+1]
  tabla <- Track_Viaje_DB %>% 
    filter(FechaViaje >= range1, FechaViaje < range2) %>% data.frame()
  
  write.csv(tabla, sprintf("Track_Viaje/Tabla_%s.csv",i), row.names = FALSE)
  rm(tabla)
  gc()
  print(i)
}

CD_ZonaEntrega <- tbl(con, "CD_ZonaEntrega") %>% data.frame()
write.csv(CD_ZonaEntrega, sprintf("Tabla_%s.csv","CD_ZonaEntrega"), row.names = FALSE)

TrazaViaje <- tbl(con, "TrazaViaje") %>% data.frame()
write.csv(TrazaViaje, sprintf("Tabla_%s.csv","TrazaViaje"), row.names = FALSE)

Track_Vertices <- tbl(con, "Track_Vertices") %>% data.frame()
write.csv(Track_Vertices, sprintf("Tabla_%s.csv","Track_Vertices"), row.names = FALSE)

Track_TipoZonas <- tbl(con, "Track_TipoZonas") %>% data.frame()
write.csv(Track_TipoZonas, sprintf("Tabla_%s.csv","Track_TipoZonas"), row.names = FALSE)

Track_Zonas <- tbl(con, "Track_Zonas")
Track_Zonas <- Track_Zonas %>% select(IdZona, NombreZona, IdTipoZona, Latitud, Longitud, Radio, UEN, VentanaServicio, VentanaHoraria) %>% collect()
write.csv(Track_Zonas, sprintf("Tabla_%s.csv","Track_Zonas"), row.names = FALSE)
