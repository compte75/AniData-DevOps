import pandas as pd
import json
import xml.etree.ElementTree as ET
import os
import logging

logger = logging.getLogger(__name__)

def convert_json_to_csv(input_path, output_path):
    """Logique spécifique pour JSON"""
    logger.info(f"Conversion JSON -> CSV pour {input_path}")
    df = pd.read_json(input_path)
    df.to_csv(output_path, index=False, encoding="utf-8")
    return output_path

def convert_xml_to_csv(input_path, output_path):
    """Logique spécifique pour XML"""
    logger.info(f"Conversion XML -> CSV pour {input_path}")
    # Utilisation de la méthode native de pandas pour le XML (nécessite lxml)
    df = pd.read_xml(input_path)
    df.to_csv(output_path, index=False, encoding="utf-8")
    return output_path

def identify_and_convert(file_path, output_dir):
    """
    Fonction Maître : Identifie le format et dirige vers le bon convertisseur.
    """
    ext = os.path.splitext(file_path)[1].lower()
    file_name = os.path.basename(file_path).replace(ext, ".csv")
    target_path = os.path.join(output_dir, f"converted_{file_name}")

    # Dictionnaire de routage (Dispatch Table)
    converters = {
        '.json': convert_json_to_csv,
        '.xml': convert_xml_to_csv,
        '.csv': lambda src, dst: pd.read_csv(src).to_csv(dst, index=False) # Simple copie/nettoyage si déjà CSV
    }

    if ext in converters:
        logger.info(f"Format détecté : {ext}")
        return converters[ext](file_path, target_path)
    else:
        raise ValueError(f"Format de fichier non supporté : {ext}")