class MigrationException(Exception):
    def __init__(self, reason: str, source_version: int, target_version: int):
        super().__init__(reason)
        self.source_version: int = source_version
        self.target_version: int = target_version
