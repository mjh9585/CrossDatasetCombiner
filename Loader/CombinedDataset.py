from typing import List, Union
from Loader.Dataset import Dataset
from enum import Enum
import pandas as pd

class CombinationMethod(Enum):
    SEQUENTIAL = 1
    INTERLACE = 2

class CombinedDataset(Dataset):
    def __init__(self, datasets : List[Dataset], 
                features : List[str], 
                method : CombinationMethod, 
                name: str = None, 
                startTime: int = None, 
                offsetTime: Union[int,List[int]] = None, 
                modifyIPs:bool =True):
                
        super().__init__(None, name, modifyIPs=modifyIPs)
        self.datasets = datasets
        self.name = name if name is not None else "Combined"
        self.features = features
        self.features.append("relTime")
        self.features.append("Dataset")
        self.method = method

        self.startTime = startTime
        self.__dsTimeOffset = 0
        self.lastTime = 0

        self.currentDataset = 0

        self.setDatasetNumber(1)

        if(self.doModifyIPs):
            for ds in self.datasets:
                ds.doModifyIPs = True

        #build a list of offset times for each dataset
        if(isinstance(offsetTime, list)):
            n = len(self.datasets)
            self.offsetTime = offsetTime[:n] + [0]*(n-len(offsetTime))
        else:
            amount = 0 if offsetTime is None else offsetTime
            self.offsetTime = [amount] * len(self.datasets)

        self.interlacedStatus = [{"df":None}] * len(self.datasets)
        # d = [
        #     {"df":df, "index":i, "len":n}
        # ]

    
    # def addPreConvertProcessor(self, preprocess):
    #     '''
    #     Adds a Preprocessor to each dataset before performing unit conversion and column renaming
    #     '''
    #     self.preProcess["preConvert"].append(preprocess)

    # def addPreCalculateProcessor(self, preprocess):
    #     '''
    #     Adds a Preprocessor to each dataset before performing additional feature calculation
    #     '''
    #     self.preProcess["preCalc"].append(preprocess)

    def combineSequential(self,nRows):
        #print(f'Combining {self.datasets[self.currentDataset].name}')
        frames = []
        count = 0
        while count < nRows and self.currentDataset < len(self.datasets):
            # attempt to load the next chunk
            try:
                d = self.datasets[self.currentDataset].getChunk(nRows-count)
                d = d[d.columns.intersection(self.features)]
            except StopIteration:
                # switch to the next dataset when end is reached
                print(f'Reached end of {self.datasets[self.currentDataset].name} with {count} flows, ',end='')
                self.currentDataset += 1
                self.__dsTimeOffset += self.lastTime    

                if(self.currentDataset < len(self.datasets)):
                    print(f'switching to {self.datasets[self.currentDataset].name}')
                else:
                    print(f'end of combined dataset')        
                continue
            
            # set startTime if not already
            if (self.startTime is None):
                self.startTime = d["Timestamp"].iloc[0]

            # find the offset used for the next dataset
            ltime = d["relTime"].max()
            if(ltime > self.lastTime):
                self.lastTime = ltime

            # recalculate timestamps
            d["relTime"] = d["relTime"] + self.__dsTimeOffset + self.offsetTime[self.currentDataset]
            d["Timestamp"] = d["relTime"] + self.startTime
            
            #label dataset source 
            if("Dataset" not in d.columns):
                d["Dataset"] = self.datasets[self.currentDataset].name

            #d = d.drop("relTime", axis=1) # limit dataset to features
            count += len(d)
            frames.append(d)
            
            # df = pd.concat([df,d],ignore_index=True)

        #join datasets together
        df = pd.concat(frames,ignore_index=True)
        if(len(df) == 0):
            raise StopIteration

        return df
    
    def calcEvenDistSizes(self,c,n):
        '''
        Calculates a list of evenly distributed amounts for an integer approximation up to a total
        '''
        r = c-(c//n*n)
        return [c//n+1 if (r-i)>0 else c//n for i in range(n)]

    def combineInterlaced(self, nRows):
        frames = []
        interlacedDS = self.datasets
        chuckSizes = self.calcEvenDistSizes(nRows, len(interlacedDS))
        offsets = self.offsetTime
        count = 0
        index = 0

        #get even chunk sizes from all the datasets until there is a complete chunk
        while count<nRows:
            if(index >= len(interlacedDS)):
                if(index == 0):
                    break
                index=0
                chuckSizes = self.calcEvenDistSizes(nRows-count, len(interlacedDS))
            
            # get the next chunk from the datasets
            ds = interlacedDS[index]
            size = chuckSizes[index]
            try:
                d = ds.getChunk(size)
                d = d[d.columns.intersection(self.features)]
            except StopIteration:
                print(f'Reached end of {ds.name}, {nRows-count} flow remaining from other datasets')
                interlacedDS.pop(index)
                chuckSizes.pop(index)
                offsets.pop(index)
                continue
            
            # set startTime if not already
            if (self.startTime is None):
                self.startTime = d["Timestamp"].iloc[0]

            # recalculate timestamps
            d["relTime"] = d["relTime"] + offsets[index]
            d["Timestamp"] = d["relTime"] + self.startTime
            
            #label dataset source
            if("Dataset" not in d.columns):
                d["Dataset"] = ds.name

            #remove dataset if end is reached, else move on to the next one
            l = len(d)
            if(l < size):
                interlacedDS.pop(index)
                chuckSizes.pop(index)
                offsets.pop(index)
                print(f'Reached end of {ds.name}, {nRows-count-l} flow remaining from other datasets')
            else:
                index+=1

            count += l
            frames.append(d)
        
        #join the datasets and sort by timestamp
        df = pd.concat(frames,ignore_index=False).sort_values(by="Timestamp")
        if(len(df) == 0):
            print(f'Reached end of Dataset')
            raise StopIteration

        return df

    def setDatasetNumber(self, num):
        for ds in self.datasets:
            num = ds.setDatasetNumber(num)+1
        return num-4
    
    def reset(self):
        for ds in self.datasets:
            ds.reset()

        self.__dsTimeOffset = 0
        self.lastTime = 0
        self.currentDataset = 0

    def getChunk(self, rows):
        if(self.currentDataset >= len(self.datasets)):
            raise StopIteration
        df = self.combineSequential(rows)
        df = self.runAdditionalPreprocess(df)
        return df

    def __iter__(self):
        return self

    def __next__(self):
        if(self.currentDataset >= len(self.datasets)):
            raise StopIteration
        if(self.method == CombinationMethod.INTERLACE):
            df = self.combineInterlaced(self.chunksize)
        else:
            df = self.combineSequential(self.chunksize)
        df = self.runAdditionalPreprocess(df)
        return df