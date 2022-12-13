import spacy
import pandas as pd
import re
import os
from utilities import *

nlp = spacy.load('en_core_web_sm')

def split_contents(dataset,split_column,verbose,file_idx,num_of_files):
    global column, ori_data, df
    dataset = dataset.drop_duplicates(subset=split_column, keep="first")
    dataset = dataset[dataset[split_column] != "[deleted]"]
    splitted_idx = []
    splitted_sentece = []
    for column in dataset.columns:
        exec('{} = {}'.format(column,[]), globals())
    for data_idx in dataset.index:
        if verbose == "yes":
            try:
                print(f'Processing data {data_idx+1} of {len(dataset)} from file {file_idx} of {num_of_files}')
            except:
                print(f'Processing data {data_idx+1} of {len(dataset)}')
        sentences = [i for i in nlp(dataset[split_column][data_idx]).sents]
        sentences_idx = 1
        for i in range(0,len(sentences)):
            if re.search('[a-zA-Z]', str(sentences[i])):
                for column in dataset.columns:
                    ori_data = dataset[column][data_idx]
                    exec('{}.append(ori_data)'.format(column),globals())
                splitted_idx.append(sentences_idx)
                splitted_sentece.append(str(sentences[i]))
                sentences_idx += 1
    split_column_name = "splitted_"+split_column
    df = pd.DataFrame({split_column_name:splitted_sentece})
    for column in dataset.columns:
        exec('df[column]={}'.format(column),globals())
    df["splitted_idx"] = splitted_idx
    new_column = df.columns.tolist()
    new_column = new_column[1:] + new_column[:1]
    df = df[new_column]
    return df