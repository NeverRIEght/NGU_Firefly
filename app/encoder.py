def encode_job(job: EncoderJobContext) -> EncoderJobContext:
    # Extract current stage
    # if PREPARED - start binary search with initial values from .env
    # if SEARCHING_CRF - start binary search with the values from the json data
    # if CRF_FOUND - perform one final encoding with the "crf_range_min" from the json data. Also, perform a check if the "crf_range_min" is the same as the "crf_range_max"
    return EncoderJobContext()
