import pandas as pd
from typing import Union
import numpy as np

def is_dummy(x: Union[pd.Series, np.ndarray]) -> bool:
    """Description. Checks if a numpy array is a dummy variable."""

    if isinstance(x, pd.Series):
        x = x.values

    x = x.astype(float)
    x = x[~np.isnan(x)]

    return np.all(np.isin(x, [0., 1.]))

def is_numeric(x: pd.Series) -> bool:
    """Description. Checks if a numpy array is numeric."""

    if x.dtype == "int64" and not is_dummy(x):  
        return True
    
    return False