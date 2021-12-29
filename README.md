# Beancount importer for Kraken

## Usage
Add repo to `importers/` directory.

Add importer to import configuration file (e.g. `personal.import`).
```
from importers import Kraken

CONFIG = [
    CoinbasePro.Importer("USD", "Assets:Kraken"),
]
```

Check with bean-identify:

```
bean-identify personal.import ./downloads
```

Import transactions with bean-extract:

```
bean-extract personal.import ./downloads > kraken.beancount
```
