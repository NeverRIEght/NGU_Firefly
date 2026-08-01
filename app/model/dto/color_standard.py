from enum import Enum


class ColorStandard(str, Enum):
    UNKNOWN = "unknown"
    BT709 = "bt709"
    BT470M = "bt470m"
    BT470BG = "bt470bg"
    SMPTE170M = "smpte170m"
    SMPTE240M = "smpte240m"
    FCC = "fcc"
    YCGCO = "ycgco"
    BT2020NC = "bt2020nc"
    BT2020C = "bt2020c"
    SMPTE2085 = "smpte2085"
    CHROMA_DERIVED_NC = "chroma-derived-nc"
    CHROMA_DERIVED_C = "chroma-derived-c"
    ICTCP = "ictcp"
    RGB = "rgb"
