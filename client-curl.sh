curl -H "Content-Type: application/json" -X POST -d '{"features":[0.0,29.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,2.0,1.0,0.0,0.0,0.0,0.0,0.5,1.0,0.0,10.0,3.0,0.3,0.3,0.3,0.0,0.0,0.0,0.0,0.0]}' http://localhost:8080/traffic/block

curl -H "Content-Type: application/json" -X POST -d '{"path":"data/corrected.gz"}' http://localhost:8080/traffic/reload