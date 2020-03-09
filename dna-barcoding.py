from pyspark import SparkConf, SparkContext
import numpy as np
import time
conf = SparkConf().setMaster("local").setAppName("DNABarcoding")
sc = SparkContext(conf = conf)

# number of dimension
n = 2
# number of population
m = 50

# def preProcess(text):
    # print(text)
#     return ("a", "1")

# data = sc.textFile("file:///SparkSkripsi/ALL_TIMUNAPEL_ITS_SAMPEL.fasta")

# processing each values and making keyValue rdd
# rdd = data.map(preProcess)

# INPUT DATA
# data array manual input for testing
data = np.array(["TCGAGACCA--AAATATAT--CGAGCGATTCGGAGAACCCG----TGAAATGAGCGGCGGCGGAC---GTCGCCGC-GA--AACATCCGCCC-CGGCC--GTCGC-------CCCC-----TCA-CCGG-AGGGGG--CGGCGACGGGGGACGGGTGAAACCCCAA-ACCGGCGCAATTC-CGCGCC-AAGG-GAAC--AATCG-AAAGA----CACGGGCCCCGCA-TCGGGG-CTCCGTGGTGTGGA-GCGGTGC--TGAGCGCCGCGCGTACT---GACACGACTCTCGGCAATGGATATCTCGGCTCTCGCATCGATGAAGAGCGCAGCGAAATGCGATACGTGGTGCGAATTGCAGAATCCCGCGAACCATCGAGTCTTTGAACGCAAGTTGCGCCCGAGGCCAACCGGCCGAGGGCACGTCCGCCTGGGCGTCAAGCGTTACGTCGCTCCGTGCCA------CG--------------------------------------------------AAGGCCC-GGACGTGCAGAGTGGCCCGTCGTGCCGGTCGGTGCGGCGGGCTGAAGAGCGGGCGATCGTCTCGATG-GCCGCGAACAACGA-GGGGTGGACTAGA-------GCTACGT-----------CGTT--GTCTCGTGCCGGC-CCGAGAGGAGATCGTGCC---CCCAGCGTGTGATCTATGCGGCGGCTGGAT", 
"TCGAGACCG-AAACTATATATCGAGCGATTCGGAGAACCCG----TGAAATGACCGGCGGC-GGCC--GCCGCCGC-GA--GACGGCCGTCC-CCGTC--GTCGC-------CCCC-----TCGTTCGG-AGGGGG--CCGCGGCGAAGGGCGGCCGAAACCCTAA-ACCGGCGCAGATT-CGCGCC-AAGG-AAAC--TATCG-AAAAA----CACGAGCCCTTCA-TCGGGC-CTTCGTGGGGCGGA-GCGGTGC-GAGCGCACCGCACGTATT---GACACGACTCTCGACAATGGATATCTCGGCTCTCGCATCGATGAAGAGCGCAGCGAAATGCGATACGTGGTGCGAATTGCAGAATCCCGCGAACCATCGAGTCTTTGAACGCAAGTTGCGCCCGAGGCCAATCGGTCGAGGGCACGTCCGCCTGGGCGTCAAGCGTTGCGCCGCTCCGTGCCGAGCCCCCA---TCCC-GCGGTAGCG-------------GGGG--TGCCG------GGCGAGGCTC-GGATGTGCAGAGTGGCCCGTCGTGCCCATCGGTGCGGCGGGCTGAAGAACGGGTTATCGTCTCATTG-GCCACGAACAACGA-GGGGTGGATGAAA----GCTGCCGCGGGCAA-GGCCTGCGTT--GTCTCGTGCCGGA-CCGGGAGAAGAATA--C----ACCCTCGTGCGATCCCATCCCA---TGCGC", 
"TCGAGACCG-AAATTATATATCGAGCGATTCGGAGAACCCG----TGAAATGAGCGGCGGC-GCCA--GCCACCGC-GA--AACGGGCGTCC-CCGTC--GTCGC-------CCCC-----TCGTTCGG-AGGGGG--CCGCGACGGAGGGCGGCCGAAACCCCAA-ACCGGCGCAGATT-GGCGCC-AAGG-AAAC--TATCG-AAAAA----CACGAGCCCTTTG-CCGGGT-CTTCGTGGGGTGGA-GCGGTGC-GAGCGCACCGCACGTATT---GACACGACTCTCGACAATGGATATCTCGGCTCTCGCATCGATGAAGAGCGCAGCGAAATGCGATACGTGGTGCGAATTGCAGAATCCCGCGAACCATCGAGTCTTTGAACGCAAGTTGCGCCCGAGGCCAATCGGTCGAGGGCACGTCCGCCTGGGCGTCAAGCGTTGCGCCGCTCCGTGCCGAGCCCCCA---TCCC-GCGGTAGCG-------------GGGG--TGCCG------GGCAAGGCTC-GGATGTGCAGAGTGGCTCGTCGTGCCCATCGGTGCGGCGGGCTGAAGAGCGGGTTATCGTCTCATTG-GCCACGAACAACGA-GGGGTGGATGAAA----GCTGCCGCGGGCAA-GGCCTGCGTT--GTCTCGT--TGGC-CCGGGAGAAGAATA--CA---CCCTACGTGCGATCCCATCCCA---TGCGC",
"TCGAGACCG-AA-TTATACATCGAGCGATTCGGAGAACCCG----TGAAATGGGCGGCGGT-CGCC--GCTGCGAA-AC-----GGCTGTCC-CCG-CCGGC--G------CCCCC-----TCGTTCGG-AGGGGG--CCGCTGCGAAGGGCGGCTGGAACCCCAA-ACCGGCGCAGATT-GGCGCC-AAGG-AAAC--TATCG-AAAAA----CACGAGCCCGTCA-TCGGGT-CTTCGTTGGGTGGA-ACGGTGC-AAGCGCACCGCACGTATT---GACACGACTCTCGACAATGGATATCTCGGCTCTCGCATCGATGAAGAGCGCAGCGAAATGCGATACGTGGTGCGAATTGCAGAATCCCGCGAACCATCGAGTCTTTGAACGCAAGTTGCGCCCGAGGCCAATCGGTCGAGGGCACGTCCGCCTGGGCGTCAAGCGTTGCGCCGCTCCGTGCCGAGTCCCCA---TCCC-GCCGTAGTG-------------GGGG--TGTCG------GGCGAGGCTC-GGATGTGCAGAGTGGCCCGTCGTGCCCATCGGCGCGGCGGGCTGAAGAGCGGGTTATCGTCTCATTG-GCCACGAACAACGA-GGGGTGGTTGAAA----GCTGCCGCGGGCAA-GGCCTGCGTT--GTCTCGTGCCGTC-CCGAGAGAAGAATA--CA---CCCTTCGTGCGATCCCATACCA---TGCGC",
"TCGAGACCG-AAATTATAT--CGAGCGATTCGGAGAACCTG----TGAAATGACCGGCGGC-GGCT--GCCGACGC-GA--AACGGCCGTCC-CTGTC--GGCGC-------CCCC-----TCGTCCGG-AGGGGG--CCGCGGGGAGGGGCGGCTGGAACCCCAA-ACCGGCGCAGACT-TGCGCC-AAGG-AAAC--TATCG-AAGAA----CACGAGCCCGTCA-TCGGGT-CTTCGTGGGGTGGG-GCGGTGC-T-GCGCACCGCACACATT---GACACGACTCTCGACAATGGATATCTCGGCTCTCGCATCGATGAAGAGCGCAGCGAAATGCGATACGTGGTGCGAATTGCAGAATCCCGCGAACCATCGAGTCTTTGAACGCAAGTTGCGCCCGAGGCCAATCGGTCGAGGGCACGTCCGCCTGGGCGTCAAGCGTTGCGTCGCTCCGTGCCGAGTCCCCA---TCCC-GCCGCAGGG-------------G-----TCGGGGGTCGGGGCGAGGCTC-GGATGTGCAGGGTGGCCCGTCGTGCCCATCGGCGCGGCGGGCTGAAGAGCGGGTTATCTTCTCATTG-GCCACGAGCAACGA-GGGGTGGATAAAG-----CTGCCTCGGGCAG-GGCCTGCGTT--GTCTCGTGCCGGC-CCGAGAGAAGAATA--CA---ACCTTCGTGCGATCCCATCCCA---GGCGC"])

# the pattern length for the dna barcode
patternLenght = 100

# param rangeVar a numpy array containing range of variables
def PSO (function, optimType, numVar, numPopulation, maxIter, rangeVar,  Vmax=2, ci=1.49445, cg=1.49445, w=0.729):
    # return number of column
    dimension = rangeVar.shape[1]

    # get each row from rangeVar for lowbound of random number in generateRandom()
    lowerBound = rangeVar[0,:]
    upperBound = rangeVar[1,:]

    if dimension == 1:
        print("it's in: ", dimension)
        dimension = numVar
        print("after: ", dimension)
    
    particles = generateRandom(numPopulation, dimension, lowerBound, upperBound)
    Gbest = calcBest(optimType, function, particles)
    Lbest = particles

    velocity = generateRandom(numPopulation, dimension, -Vmax, Vmax)

    bestParticle = enginePSO(optimType, function, maxIter, lowerBound, upperBound, Vmax, ci, cg, w, Gbest, Lbest, particles, velocity)
    return bestParticle

def enginePSO(optimType, function, maxIter, lowerBound, upperBound, Vmax, ci, cg, w, Gbest, Lbest, particles, velocity):
    FLbest = calcFitness(optimType, function, Lbest)
    # print("======================== FLBEST ========================")
    # print(FLbest)
    FGbest = optimType*function(Gbest)
    # print("Gbest: ", Gbest)
    curve = np.zeros(maxIter)
    for t in range(maxIter):
        for i in range(particles.shape[0]):
            # print("======================== LOOP", t,i, "========================")
            # print("particles: \n", particles)
            for d in range(particles.shape[1]):
                ri = np.random.uniform(size=1)
                rg = np.random.uniform(size=1)
                # print("ri: ", ri, "rg: ", rg)
                
                # print("w:",w,"velocity[",i,",",d,"]:", velocity[i,d],"ci:",ci,"ri:",ri,"Lbest[",i,",",d,"]:",Lbest[i,d],"particles[",i,",",d,"]:",particles[i,d],"cg:",cg,"rg:",rg,"Gbest[",d,"]:",Gbest[d])
                newVelocity = w * velocity[i,d] + ci*ri*(Lbest[i,d]-particles[i,d]) + cg*rg*(Gbest[d]-particles[i,d])
                # print("newVelo: ", newVelocity, "velocity[",i,",",d,"] before: ", velocity[i,d])

                if newVelocity < -Vmax: 
                    newVelocity = -Vmax
                if newVelocity > Vmax: 
                    newVelocity = Vmax
                velocity[i,d] = newVelocity
                # print("newVelo: ", newVelocity, "velocity[",i,",",d,"] after: ", velocity[i,d])

                newPosition = particles[i,d] + velocity[i,d]
                # print("newPosition: ", newPosition, "particles[",i,",",d,"] before:", particles[i,d])

                if len(lowerBound)==1 :
                    if newPosition < lowerBound:
                        newPosition = lowerBound
                    if newPosition > upperBound:
                        newPosition = upperBound
                else:
                    if newPosition < lowerBound[d]:
                        newPosition = lowerBound[d]
                    if newPosition > upperBound[d]:
                        newPosition = upperBound[d]
                
                particles[i,d] = newPosition
                # print("newPosition: ", newPosition, "particles[",i,",",d,"] after:", particles[i,d])

                fun = optimType*function(particles[i,:])
                # print("F: ", f, "FLbest: ", FLbest[i])
                if fun < FLbest[i]:
                    # print("HIT F: ", f, "< FLbest: ", FLbest[i])
                    Lbest[i,:] = particles[i,:]
                    FLbest[i] = fun
                    # print("FLbest[i]: ", FLbest[i], "FGbest: ", FGbest)
                    if FLbest[i] < FGbest:
                        # print("HIT 2 FLbest[i]: ", FLbest[i], "< FGbest: ", FGbest)
                        Gbest = Lbest[i,:]
                        # print("Gbest: ", Gbest)
                        FGbest = FLbest[i]
        curve[t] = FGbest
    curve <- curve*optimType
    return Gbest

# create matrix or an array of np array for given bound (upper and lower) then return it
def generateRandom(numPopulation, dimension, lowerBound, upperBound):
    if isinstance(lowerBound, list):
        result = np.empty((numPopulation, dimension))
        for i in range(dimension):
           result[:,i] = np.random.uniform(lowerBound[i],upperBound[i],numPopulation)
    else:
       result = np.random.uniform(lowerBound,upperBound,(numPopulation, dimension))
        
    return result

# calculate fitness function
def calcFitness(optimType, function, population):
    fitness = np.zeros(population.shape[0])
    # loop for number of rows
    for i in range(population.shape[0]):
        # print(population[i,:])
        fitness[i] = optimType*function(population[i,])
        # print("fitness[",i,"]: ", fitness[i])

    return fitness

def calcBest(optimType, function, population):
    fitness = calcFitness(optimType, function,population)
    best = population[np.argmax(fitness),:]

    return best

# Test Function
def function1(x):
    return sum(x**2)

def function2(x):
    return sum(abs(x) + np.prod(abs(x)))

# Main
numIter = [100, 500, 1000]
numPopu = [10, 30, 50, 100]
numVar = 10
rangeVar = np.array([[-100.00], [100.00]]) 
optimumValue = np.empty(len(numIter)*len(numPopu))
timeElapsed = np.empty(len(numIter)*len(numPopu))
i = 0
for maxIter in numIter:
    for numPopulation in numPopu:
        np.random.seed(1)  
        start_time = time.time()
        result = PSO(function2, 1, numVar, numPopulation, maxIter, rangeVar)
        optimumValue[i] = function2(result)
        # print("Result: \n", result)
        # print("Optimum Value: ", function1(result))
        # print("Time Elapsed: \n", time_elapsed)
        timeElapsed[i] = time.time() - start_time
        i+=1


print(optimumValue)
print(timeElapsed)