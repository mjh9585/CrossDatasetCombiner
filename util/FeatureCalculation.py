
## Feature calculations
### Bytes/s
def featureBytesPerSec(df):
    return calcNumPerSec(df['Total Length of Packets'],df['Flow Duration'])

def featureBytesPerSecFwd(df):
    return calcNumPerSec(df['Total Length of Fwd Packets'],df['Flow Duration'])

def featureBytesPerSecBwd(df):
    return calcNumPerSec(df['Total Length of Bwd Packets'],df['Flow Duration'])

## Pkts/s
def featurePktsPerSec(df):
    return calcNumPerSec(df['Total Packets'],df['Flow Duration'])

def featurePktsPerSecFwd(df):
    return calcNumPerSec(df['Total Fwd Packets'],df['Flow Duration'])

def featurePktsPerSecBwd(df):
    return calcNumPerSec(df['Total Bwd Packets'],df['Flow Duration'])

### Total Len
def featureTotalLenPkts(df):
    return df['Total Length of Fwd Packets'] + df['Total Length of Bwd Packets']

### Total Pkts
def featureTotalPkts(df):
    return df['Total Fwd Packets'] + df['Total Bwd Packets']

### ID
def featureID(df):
    #return df.apply(lambda row: f'{row["Src IP"]}-{row["Dst IP"]}-{row["Src Port"]}-{row["Dst Port"]}', axis=1)
    return [f'{sI}-{dI}-{sP}-{dP}' for sI,dI,sP,dP in zip(df["Src IP"],df["Dst IP"],df["Src Port"],df["Dst Port"])]

### Mean
def featurePktLenMean(df):
    return (df['Fwd Packet Length Mean']*df['Total Fwd Packets'] + df['Bwd Packet Length Mean']*df['Total Bwd Packets'])/(df['Total Packets']*1.0)

def featureIATMean(df):
    return (df['Fwd IAT Mean']*df['Total Fwd Packets'] + df['Bwd IAT Mean']*df['Total Bwd Packets'])/(df['Total Packets']*1.0)

### Ratio
def featureDownUpRatio(df):
    return df['Total Bwd Packets']/(df['Total Fwd Packets']*1.0)

## base methods
def calcNumPerSec(size, duration):
    return size/duration