# Shreyas' Analytical Solution

### Question
A client has 6 shipments on 6 tradelanes in a week. If the shipments occur on any of the tradelanes randomly, with all possibilities equally likely, what is the probability that some tradelane had more than 2 shipments?

### Answer

```
P(At least one tradelane has more than 2 shipments)

= 1 - P(No tradelane has more than 2 shipments)

= 1 - (P(all tradelanes have exactly 1 shipment)
     + P(1 or more tradelanes have exactly 2 shipments and
       no tradelane has more than 2 shipments)

```

#### Total number of ways to organize 6 shipments on 6 tradelanes
6^6 = ```46,656``` ways

#### Number of ways that all tradelanes have exactly 1 shipment
6! = ```720``` ways

#### Number of ways that one or more tradelanes have 2 shipments and no tradelane has more than 2 shipments
- you have tradelanes ```a,b,c,d,e,f```
- WLOG this can happen in 3 forms
  - **A:** ```a,a,b,c,d,e```
  - **B:** ```a,a,b,b,c,d```
  - **C:** ```a,a,b,b,c,c```

**Scenario A**
- Pick the tradelane that will have 2 shipments
  - ```6c1 = 6```
- Pick the 2 shipments to be together
  - ```6c2 = 15```
- Pick 4 of the 5 remaining tradelanes to have exactly one shipment each respectively, and order the remaining 4 shipments on the 4 selected tradelanes
  - ```5c4 * 4! = 5p4 = 5*4*3*2 = 120```

Product = ```10,800``` ways

**Scenario B**
- Pick the 2 tradelanes that will have 2 shipments each; call them ```a``` and ```b```
  - ```6c2 = 15```
- Pick the 2 shipments to go on tradelane ```a```
  - ```6c2 = 15```
- Pick the 2 shipments to go on tradelane ```b```
  - ```4c2 = 6```
- Pick 2 of the 4 remaining tradelanes to have exactly one shipment each respectively, and order the remaining 2 shipments on the 2 selected tradelanes
  - ```4c2 * 2! = 4p2 = 4*3 = 12```

Product = ```16,200``` ways

**Scenario C**
- Pick the 3 tradelanes that will have 2 shipments each; call them ```a```, ```b```, and ```c```
  - ```6c3 = 20```
- Pick the 2 shipments to go on tradelane ```a```
  - ```6c2 = 15```
- Pick the 2 shipments to go on tradelane ```b```
  - ```4c2 = 6```
- Pick the 2 shipments to go on tradelane ```c```
  - ```2c2 = 1```

Product = ```1,800``` ways

### Final Answer
```
P(At least one tradelane has more than 2 shipments)

= 1 - [(720 + 10,800 + 16,200 + 1,800) / 46,656]

= 1 - 0.6327

= 0.3673

```
