-- Final-Layer in HDFS: bereinigte Karten, eine Zeile pro Druckversion.
-- Speicherort: /user/hadoop/mtg/final/cards (Parquet)
-- Die Spark-Jobs registrieren die Tabelle mit dieser Datei; raw_to_final.py
-- schreibt genau diese Spalten und Typen, final_to_mongodb.py liest sie.
CREATE TABLE final_cards (
    id              STRING         COMMENT 'ID der API, eindeutig je Druckversion',
    multiverseid    BIGINT         COMMENT 'Gatherer-ID (fehlt bei manchen Online-/Promo-Karten)',
    name            STRING         COMMENT 'Englischer Kartenname',
    mana_cost       STRING         COMMENT 'Manakosten, z. B. {2}{U}',
    cmc             DOUBLE         COMMENT 'Umgerechnete Manakosten',
    colors          ARRAY<STRING>  COMMENT 'Farben',
    color_identity  ARRAY<STRING>  COMMENT 'Farbidentität',
    type_line       STRING         COMMENT 'Typzeile, z. B. Creature — Merfolk',
    supertypes      ARRAY<STRING>,
    types           ARRAY<STRING>,
    subtypes        ARRAY<STRING>,
    rarity          STRING,
    set_code        STRING         COMMENT 'Kürzel der Edition, z. B. TMP',
    set_name        STRING,
    text            STRING         COMMENT 'Regeltext (englisch)',
    flavor          STRING         COMMENT 'Flavor-Text (englisch)',
    artist          STRING,
    number          STRING         COMMENT 'Sammlernummer innerhalb der Edition',
    power           STRING,
    toughness       STRING,
    loyalty         STRING,
    layout          STRING,
    image_url       STRING         COMMENT 'Kartenbild (Gatherer, https)',
    printings       ARRAY<STRING>  COMMENT 'Alle Editionen, in denen die Karte erschienen ist',
    name_de         STRING         COMMENT 'Deutscher Kartenname',
    type_de         STRING,
    text_de         STRING,
    flavor_de       STRING,
    multiverseid_de BIGINT,
    image_url_de    STRING
)
USING PARQUET
LOCATION '${location}'
