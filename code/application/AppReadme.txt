inputs:
  crimes.txt
    (community)
  taxi.txt
    (communityPickup,communityDropoff)
  coffee.txt
    (community)

transformation:
  crimes
    (community,count)
    (community,count,sum)
    (community,rate)
  distance
    (community1,community2,distance)
  geo
    (community,geo)
  taxi
    (pickup,dropoff)
    (pickup,dropoff,count)
    use taxi_help.txt to insert missing data
    (pickup,dropoff,count,sum)
    (pickup,dropoff,weight)
    (dropoff,taxi)
  coffee
    (community)
    use coffee_help.txt to insert missing data
    (community,count)

output:
  prediction
    (community,predictedRate)
