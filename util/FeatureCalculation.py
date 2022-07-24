
### Feature calculations
def featureBytesPerSec(df):
    pass

def featureBytesPerSecFwd(df):
    return calcBytesPerSec(df['Total Length of Fwd Packets'],df['Flow Duration'])

def featureBytesPerSecBwd(df):
    return calcBytesPerSec(df['Total Length of Bwd Packets'],df['Flow Duration'])

def featureTotalLenPkts(df):
    return df['Total Length of Fwd Packets'] + df['Total Length of Bwd Packets']

def featureTotalPkts(df):
    return df['Total Fwd Packets'] + df['Total Bwd Packets']

### base methods
def calcBytesPerSec(size, duration):
    return size/duration