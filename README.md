# Challenges of capturing engagement on Facebook for Altmetrics
> Code and data accompanying the short paper to be presented at STI 2018 (http://sti2018.cwts.nl/).

[![DOI](https://zenodo.org/badge/125935481.svg)](https://zenodo.org/badge/latestdoi/125935481)

Authors: Asura Enkhbayar, Juan Pablo Alperin


For more details about the overall project about Facebook & Altmetrics be sure to visit this repository: [https://github.com/ScholCommLab/facebook-hidden-engagement](https://github.com/ScholCommLab/facebook-hidden-engagement)


## Data

We used the Web of Science dataset used in Piwowar et al. (2017) as our input dataset to explore the challenges around collecting private engagement on Facebook.

The Web of Science dataset is available at: [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1041791.svg)](https://doi.org/10.5281/zenodo.1041791)

## Code

To install required packages run `pip install -r requirements.txt`. The latest version of the Python SDK for Facebook can be installed via `pip install -e git https://github.com/mobolic/facebook-sdk.git#egg=facebook-sdk`.

1. Resolving DOIs

    `doi-resolver.py` is a CLI-script that takes a CSV file as input containing a list of DOIs. These DOIs are then resolved to acquire the current landing pages.

    _Disclaimer_: Be aware that mass-resolving a big amount of DOIs can lead to problems with publishers. Be sure to adhere to best practices of web crawling.

2. Collect engagement for URL variations

    `query-urls-fb.py` is a CLI script that uses the previously created file to create 4 URL variations and collects the engagement numbers using the Facebook Graph API.

3. `wos-analysis.ipynb` contains the analysis and breakdown into 3 problem cases.
## References

Piwowar, H., Priem, J., Larivière, V., Alperin, J. P., Matthias, L., Norlander, B., Farley, A., et al. (2017). The State of OA: A large-scale analysis of the prevalence and impact of Open Access articles. _PeerJ_.