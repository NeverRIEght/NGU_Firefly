from enum import Enum


class HdrFormat(str, Enum):
    SDR = "sdr"
    HDR10 = "hdr10"
    HDR10_PLUS = "hdr10plus"
    HLG = "hlg"
    DOLBY_VISION = "dolby_vision"
