"""Dealer configuration loading from YAML or fallback to built-in."""

from pathlib import Path
from typing import Any, Dict, List, Optional

from .settings import settings

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class DealerConfig:
    """Dealer configuration model."""

    def __init__(
        self,
        business_id: str,
        name: str,
        location: str,
        url: str,
        email: str = "",
        phone: str = "",
    ) -> None:
        self.business_id = business_id
        self.name = name
        self.location = location
        self.url = url
        self.email = email
        self.phone = phone

    def to_dict(self) -> Dict[str, Any]:
        return {
            "business_id": self.business_id,
            "name": self.name,
            "location": self.location,
            "url": self.url,
            "email": self.email,
            "phone": self.phone,
        }


# Built-in dealer list (fallback when config file is missing)
DEFAULT_DEALERS: List[Dict[str, Any]] = [
    {"business_id": "RIDDERMARK_OSTERSUND", "name": "Riddermark Bil - Östersund", "location": "Östersund",
     "url": "https://www.blocket.se/mobility/search/car?orgId=5359029", "email": "ostersund@riddermarkbil.se", "phone": "010-330 73 99"},
    {"business_id": "RIDDERMARK_GOTEBORG", "name": "Riddermark Bil - Göteborg", "location": "Göteborg",
     "url": "https://www.blocket.se/mobility/search/car?orgId=7903992", "email": "goteborg@riddermarkbil.se", "phone": "030-350 36 00"},
    {"business_id": "RIDDERMARK_HALMSTAD", "name": "Riddermark Bil - Halmstad", "location": "Halmstad",
     "url": "https://www.blocket.se/mobility/search/car?orgId=1663231", "email": "halmstad@riddermarkbil.se", "phone": "035-240 06 00"},
    {"business_id": "RIDDERMARK_JARFALLA", "name": "Riddermark Bil - Järfälla", "location": "Järfälla",
     "url": "https://www.blocket.se/mobility/search/car?orgId=188150", "email": "jarfalla@riddermarkbil.se", "phone": "08-572 142 40"},
    {"business_id": "RIDDERMARK_LINKOPING", "name": "Riddermark Bil - Linköping", "location": "Linköping",
     "url": "https://www.blocket.se/mobility/search/car?orgId=271166", "email": "linkoping@riddermarkbil.se", "phone": "013-480 22 00"},
    {"business_id": "RIDDERMARK_LANNA", "name": "Riddermark Bil - Länna", "location": "Skogås",
     "url": "https://www.blocket.se/mobility/search/car?orgId=912855", "email": "skogas@riddermarkbil.se", "phone": "08-586 269 00"},
    {"business_id": "RIDDERMARK_MEGASTORE", "name": "Riddermark Bil Megastore - Strängnäs", "location": "Strängnäs",
     "url": "https://www.blocket.se/mobility/search/car?orgId=4438739", "email": "megastore@riddermarkbil.se", "phone": "08-591 122 30"},
    {"business_id": "RIDDERMARK_NACKA", "name": "Riddermark Bil - Nacka", "location": "Nacka",
     "url": "https://www.blocket.se/mobility/search/car?orgId=4801089", "email": "nacka@riddermarkbil.se", "phone": "08-36 08 27"},
    {"business_id": "RIDDERMARK_RECHARGE", "name": "Riddermark bil - Recharge Stockholm", "location": "Stockholm",
     "url": "https://www.blocket.se/mobility/search/car?orgId=1322132", "email": "recharge@riddermarkbil.se", "phone": "08-36 08 26"},
    {"business_id": "RIDDERMARK_TRANSPORTBILAR", "name": "Riddermark trasportbilar - Strängnäs", "location": "Strängnäs",
     "url": "https://www.blocket.se/mobility/search/car?orgId=4438739", "email": "transportbilar@riddermarkbil.se", "phone": "08-522 227 88"},
    {"business_id": "RIDDERMARK_SUNDSVALL", "name": "Riddermark Bil - Sundsvall", "location": "Sundsvall",
     "url": "https://www.blocket.se/mobility/search/car?orgId=6228526", "email": "sundsvall@riddermarkbil.se", "phone": "010-129 59 68"},
    {"business_id": "RIDDERMARK_TABY", "name": "Riddermark Bil - Täby", "location": "Täby",
     "url": "https://www.blocket.se/mobility/search/car?orgId=5665308", "email": "taby@riddermarkbil.se", "phone": "08-583 502 90"},
    {"business_id": "RIDDERMARK_UPPSALA", "name": "Riddermark Bil - Uppsala", "location": "Uppsala",
     "url": "https://www.blocket.se/mobility/search/car?orgId=1596271", "email": "uppsala@riddermarkbil.se", "phone": "018-470 74 00"},
    {"business_id": "RIDDERMARK_VASTERAS", "name": "Riddermark Bil - Västerås", "location": "Västerås",
     "url": "https://www.blocket.se/mobility/search/car?orgId=9058894", "email": "vasteras@riddermarkbil.se", "phone": "021-540 08 00"},
    {"business_id": "RIDDERMARK_OREBRO", "name": "Riddermark Bil - Örebro", "location": "Örebro",
     "url": "https://www.blocket.se/mobility/search/car?orgId=7429108", "email": "orebro@riddermarkbil.se", "phone": "019-760 88 77"},
]


def load_dealers(config_path: Optional[Path] = None) -> List[DealerConfig]:
    """
    Load dealer configuration from YAML file or use built-in defaults.

    Args:
        config_path: Path to dealers YAML. Defaults to config/dealers.yaml.

    Returns:
        List of DealerConfig objects.
    """
    path = config_path or (settings.base_dir / settings.config_dir / "dealers.yaml")

    if path.exists() and YAML_AVAILABLE:
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            dealers_raw = data.get("dealers", [])
            return [_dealer_from_dict(d) for d in dealers_raw]
        except Exception:
            pass

    return [_dealer_from_dict(d) for d in DEFAULT_DEALERS]


def _dealer_from_dict(d: Dict[str, Any]) -> DealerConfig:
    return DealerConfig(
        business_id=d["business_id"],
        name=d["name"],
        location=d["location"],
        url=d["url"],
        email=d.get("email", ""),
        phone=d.get("phone", ""),
    )
