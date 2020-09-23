# Link Predictor using GraphX

## Generate ranked scores

```bash
# Jaccard coefficient
spark-submit --class ca.uwaterloo.cs451.project.JaccardCoefficient  --num-executors 4 --executor-cores 4 --executor-memory 24G  target/assignments-1.0.jar  --vert project-data/vertices.txt  --eold project-data/edges_old.txt  --output ranked-scores-jaccard-coefficient

# First 10 lines of the list
cat ranked-scores-jaccard-coefficient/* | head -10
((16132,16258),1.0)
((6676,7490),1.0)
((6066,6396),1.0)
((6066,10598),1.0)
((7628,8294),1.0)
((7628,12600),1.0)
((2412,10576),1.0)
((118,3096),1.0)
((3148,4222),1.0)
((3148,3922),1.0)
```

## Evaluate predictor result

```bash
spark-submit --class ca.uwaterloo.cs451.project.EvaluatePredictions  --num-executors 4 --executor-cores 4 --executor-memory 24G  target/assignments-1.0.jar  --vert project-data/vertices.txt  --eold project-data/edges_old.txt  --enew project-data/edges_new.txt --rank ranked-scores-jaccard-coefficient
...
653.0/15208
correctness=0.042937927
...
```
