import toml


class Config:
    def __init__(self, provider):
        self._config = self._load_config('C:\\Users\\miroslaw-steblik\\my-python-projects\\white\\.config\\config.toml')
        self.provider = provider

    def _load_config(self, config_file):
        return toml.load(config_file)
    
    def _set_config(self):
        self.config = self._config

    @property
    def provider_name(self):
        return self.provider

    @property
    def provider_weekly_file(self):
        if self.provider == 'Aviva':
            return self._config['rebalancing_monitoring']['aviva']['source']
        elif self.provider == 'ScottishWidows':
            return self._config['rebalancing_monitoring']['scottishwidows']['source']
        else:
            raise ValueError("Provider not found")
    @property
    def provider_reference_file(self):
        if self.provider == 'Aviva':
            return self._config['rebalancing_monitoring']['aviva']['reference']
        elif self.provider == 'ScottishWidows':
            return self._config['rebalancing_monitoring']['scottishwidows']['reference']
        else:
            raise ValueError("Provider not found")

    @property
    def provider_static_funds_targets_file(self):
        if self.provider == 'Aviva':
            return self._config['rebalancing_monitoring']['aviva']['static_funds_targets']
        elif self.provider == 'ScottishWidows':
            return self._config['rebalancing_monitoring']['scottishwidows']['static_funds_targets']
        else:
            raise ValueError("Provider not found")
    @property
    def provider_output_file(self):
        if self.provider == 'Aviva':
            return self._config['rebalancing_monitoring']['aviva']['output']
        elif self.provider == 'ScottishWidows':
            return self._config['rebalancing_monitoring']['scottishwidows']['output']
        else:
            raise ValueError("Provider not found")

    @property
    def webhook_url(self):
        return self._config['rebalancing_monitoring']['webhook_url']
    @property
    def output_table_file(self):
        return self._config['rebalancing_monitoring']['output_table']  
    @property
    def date_format(self):
        if self.provider == 'Aviva':
            return "%d/%m/%Y"
        elif self.provider == 'ScottishWidows':
            return "%d-%b-%y" # 01-Jan-21
        else:
            raise ValueError("Provider not found")
        
    @property
    def add_extra_month(self):
        if self.provider == 'Aviva':
            return 'YES'
        elif self.provider == 'ScottishWidows':
            return 'NO'
        else:
            raise ValueError("Provider not found")
    
    @property
    def in_percent(self):
        if self.provider == 'Aviva':
            return 'NO'
        elif self.provider == 'ScottishWidows':
            return 'YES'
        else:
            raise ValueError("Provider not found")
        
    @property
    def range_min(self):
        return -0.03
    @property
    def range_max(self):
        return 0.03
    
    @property
    def key_word(self):
        if self.provider == 'Aviva':
            return None
        elif self.provider == 'ScottishWidows':
            return '_M'
        
    @property
    def client_name(self):
        if self.provider == 'Aviva':
            return 'Rebalancing-Aviva'
        elif self.provider == 'ScottishWidows':
            return 'Rebalancing-ScottishWidows'
        else:
            raise ValueError("Provider not found")






