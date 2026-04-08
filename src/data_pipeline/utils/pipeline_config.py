from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class ConditionOperator(Enum):
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    EQ = "="
    NEQ = "!="
    IN = "in"
    NOT_IN = "not_in"


class ConditionFailBehavior(Enum):
    SKIP = "skip"  # si la contion passe pas, on passe à la transformation suivante
    STOP = "stop"  # si la contion passe pas, on arrête de transformer cette ligne
    ERROR = "error"  # si condition passe pas la ligne est une anomalie


class TypeTransformation(Enum):
    MULTIPLY = "multiply"  # multiplication -> fournir valeur float
    DIVIDE = "divide"  # Diviser        -> fournir valeur float
    ADD = "add"  # Additionner    -> fournir valeur float
    SUBTRACT = "subtract"  # Soustraire     -> fournir valeur float
    POWER = "power"  # Puissance      -> fournir valeur float
    ROUND = "round"  # Arrondir       -> fournir int
    FLOOR = "floor"  # arrondir à l'entier inférieur
    CEIL = "ceil"  # arrondir à l'entier supérieur
    CLIP_MIN = "clip_min"  # valeur min pour éviter les abus (si X > 110 -> x = 100)   -> fournir valeur float
    CLIP_MAX = "clip_max"  # valeur max pour éviter les abus (si X > 110 -> x = 100)   -> fournir valeur float
    AGE_TO_BIRTHDATE = (
        "age_to_birthdate"  # age (nombre) -> date de naissance au 1er janvier
    )

    UPPER = "upper"  # MAJUSCULE
    LOWER = "lower"  # minuscule
    REPLACE = "replace"  # replace - > fournir old/new
    REGEX_REPLACE = "regex_replace"  # Regex ? #TODO
    SUBSTRING = "substring"  # -> start/end

    # Pour les type array,c'est des arrays json type : [xxx, xxx, xx, xxx]
    ARRAY_UNIQUE = "array_unique"  # supprime doublons
    ARRAY_SLICE = "array_slice"  # ex [0, 1, 2]

    # Pour type date ou type datetime, mais comment savoir si c'est une date format anglais ou format fr ??
    ADD_DAYS = "add_days"  # -> fournir int
    ADD_MONTH = "add_month"
    ADD_YEAR = "add_year"  #
    EXTRACT_DAY = "extract_day"  # fournir string dans string 1
    EXTRACT_MONTH = "extract_month"  # fournir string dans string 1 et il ressort un nombre comme un 1 pour janvier
    EXTRACT_YEAR = "extract_year"  # fournir string dans string 1
    DEFAULT_DAY = "default_day"  # fournir un entier qui est un numéro de jour
    DEFAULT_MONTH = "default_month"  # fournir un entier qui est le numéro du mois
    DEFAULT_YEAR = "default_year"
    ADD_HOUR = "add_hour"
    ADD_MINUTE = "add_minute"
    ADD_SECOND = "add_second"
    EXTRACT_HOUR = "extract_hour"
    EXTRACT_MINUTE = "extract_hour"
    EXTRACT_SECOND = "extract_hour"


class TypeDonnees(Enum):
    INT = "int"
    DECIMAL = "decimal"
    STRING = "string"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BOOLEAN = "boolean"
    ARRAY_DELIMITED_JSON = "array_delimited_json"  # → "item1;item2;item3"
    ARRAY_JSON = "array_json"  # → '["item1","item2"]'


class ExtensionFichier(Enum):
    CSV = "csv"
    JSON = "json"


@dataclass
class ETLTransformationCondition:
    id_condition: int
    id_transformation: int
    groupe_code: int
    operator: ConditionOperator
    value_num: Optional[float] = None
    value_str: Optional[str] = None
    value_date: Optional[datetime] = None


@dataclass
class ETLColumnTransformation:
    id_transformation: int
    id_etl_column_mapping: int
    ordre: int
    # skip_error: bool
    type_transformation: TypeTransformation
    condition_fail_behavior: ConditionFailBehavior
    value_num: Optional[float] = None
    value_int: Optional[int] = None
    value_str: Optional[str] = None
    value_int_2: Optional[int] = None
    value_str_2: Optional[str] = None
    conditions: list[ETLTransformationCondition] = field(default_factory=list)


@dataclass
class ColumnConstraint:
    id_constraint: int


@dataclass
class StringConstraint(ColumnConstraint):
    min_length: int = 0
    max_length: Optional[int] = None


@dataclass
class NumericConstraint(ColumnConstraint):
    nb_min: Optional[float] = None
    nb_max: Optional[float] = None
    nb_decimal: Optional[int] = None


@dataclass
class DateConstraint(ColumnConstraint):
    date_min: Optional[datetime] = None
    date_max: Optional[datetime] = None


@dataclass
class ETLColumnMapping:
    id_etl_column_mapping: int
    colonne_bdd: str
    colonne_fichier: str
    in_file: bool
    type_donnees: TypeDonnees
    nullable: bool
    valeur_defaut: Optional[str]
    unique_constraint: bool
    constraint: Optional[ColumnConstraint] = None
    transformations: list[ETLColumnTransformation] = field(default_factory=list)


@dataclass
class PipelineETL:
    id_etl_pipeline: int
    libelle: str
    table_nom: str
    dossier_emplacement: str
    nom_fichier_fixe: str
    nom_fichier_variable: str
    extension_fichier: ExtensionFichier
    dossier_clean_emplacement: str
    active: bool
    colonnes: list[ETLColumnMapping] = field(default_factory=list)
