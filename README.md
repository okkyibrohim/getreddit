# GetReddit
A simple Python library to collect and filters Reddit data (submissions or comments) for re-training language model needs.
## About this repository
Training or re-training language model for domain adaption sometimes needs a huge raw dataset. Reddit (https://www.reddit.com/) is one of the best places to collect dataset for domain adaption of the language model since there are various topic discussed there which is collected in one place called a "Subreddit". Pushshift (https://github.com/pushshift/api) are providing a great API to collect Reddit datasets based on your need but it is limited to a maximum of 500 submissions/comments per hit. To get more data, they provide a link to download full submissions (https://files.pushshift.io/reddit/submissions/) or comments (https://files.pushshift.io/reddit/comments/) from each month. This Python library is created to make us easily download, filter, and split to sentence the Reddit submissions/comments provided by Pushshift. 
## Main use
In this Python library, we provide 3 different `mode` that you can use: <br />
1. `download`: This `mode` type is used to download and filter the Reddit submissions/comments data from a particular month in one run.
2. `filter`: This `mode` type is used to filter the downloaded Reddit submissions/comments data from a particular month in one run.
3. `split`: This `mode` type is used to split a specific attribute Reddit submissions/comments from into a separate sentence.
## Requirements
This Python library is implemented in `Python 3` and requires a number of packages. To install all needed packages, simply run `$ pip install -r requirements.txt` in your virtual environtment. To be able to use the `split` `mode`, you need to download the model `en_core_web_sm` from Spacy, simply by scripting `$ python -m spacy download en_core_web_sm` on your virtual environment. Not that, this may only work in English. For other languages, you need to edit the Spacy model that is used to split the data. We recommend you to use `Python 3.10.2` version as we use it to develop it. Other `Python 3` version may need several modification in the package used.
