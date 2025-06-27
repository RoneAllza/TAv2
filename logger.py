import logging

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("emission_pipeline.log", mode='a'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("EmissionPipeline")
