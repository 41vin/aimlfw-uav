model_name=uav-model
curl -v -H "Host: $model_name.kserve-test.example.com" http://192.168.190.140:32576/v1/models/$model_name:predict -d @./input_uav.json
