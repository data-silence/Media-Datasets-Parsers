import requests
import pandas as pd

headers = {
	"user-agent": "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser; GTB5; Mozilla/4.0 ("
	"compatible; MSIE 6.0; Windows NT 5.1; SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618) "
}

smi_name = {'vedomosti'}

for site_name in smi_name:
	url = 'https://tg.i-c-a.su/json/' + site_name

	answer = requests.get(url, headers=headers).json()['messages'][2]['message']
	print(answer.replace('<br />', ''))
	# print(answer.strip('<br />'))
