import meic
import random
 
# text colours
green = '\033[92m'
blue = '\033[34m'
red = '\033[31m'
reset = '\033[0m'

timestamp_ranges = [(930, 959), (1000, 1059), (1100, 1159), (1200, 1259)]
stop_and_limit = [(0.75, 0.65), (0.80, 0.65), (0.75, 0.60)]

def generate_random_arguments():
    # generate random timestamps between 9:30 and 16:00
    timestamps = random.randint(*timestamp_ranges[random.randint(0, 3)]) * 100000

    # generate random arguments
    spread_width = random.randint(2, 12) * 5  # between 10 and 60 and a multiple of 5
    num_spreads = random.randint(3, 3)        # between 1 and 5
    stop_loss = random.randint(5, 12) * 0.25  # between 1.25x and 3x and a multiple of 0.25
    entry_credit = round(random.uniform(1, 5) * 4) / 4 # between 1 and 5 and a multiple of 0.25
    
    random_item = random.choice(stop_and_limit)
    stop_price = round(entry_credit * random_item[0] * 10) / 10
    limit_price = round(entry_credit * random_item[1] * 10) / 10
    return timestamps, spread_width, num_spreads, stop_loss, entry_credit, stop_price, limit_price

#--------------------------------------------------------------#
#-----------------------GENETIC ALGORITHM----------------------#
#--------------------------------------------------------------#
CROSSOVER_PROBABILITY = 0.5
MUTATION_PROBABILITY = 0.15
GENERATIONS = 100

class Individual:
    def __init__(self, timestamps, spread_width, num_spreads, stop_loss, entry_credit, stop_price, limit_price):
        self.timestamps = timestamps
        self.spread_width = spread_width
        self.num_spreads = num_spreads
        self.stop_loss = stop_loss
        self.entry_credit = entry_credit
        self.stop_price = stop_price
        self.limit_price = limit_price
        
    def mutate(self):
        # mutate timestamps
        if random.random() < MUTATION_PROBABILITY:
            self.timestamps = [random.randint(*timestamp_ranges[random.randint(0, 3)]) * 100000 for _ in range(1)]
                
        # mutate spread width
        if random.random() < MUTATION_PROBABILITY:
            self.spread_width = random.randint(2, 12) * 5
        
        # mutate num_spreads
        if random.random() < MUTATION_PROBABILITY:
            self.num_spreads = random.randint(3, 3)
                    
        # mutate stop loss multiplier
        if random.random() < MUTATION_PROBABILITY:
            self.stop_loss = random.randint(5, 12) * 0.25
            
        # mutate entry credit
        if random.random() < MUTATION_PROBABILITY:
            self.entry_credit = round(random.uniform(1, 5) * 4) / 4
            
            random_item = random.choice(stop_and_limit)
            self.stop_price = round(self.entry_credit * random_item[0] * 10) / 10
            self.limit_price = round(self.entry_credit * random_item[1] * 10) / 10
            
    def calculate_fitness(self):
        return meic.argument_input(self.timestamps, self.spread_width, self.entry_credit, self.num_spreads, self.stop_price, self.limit_price, self.stop_loss)

# randomly generate an initial population
def generate_initial_population():
    return [Individual(*generate_random_arguments()) for _ in range(20)]

def select_individuals(population, fitnesses):
    winrates, profits = zip(*fitnesses)

    profits = list(profits)
    lowest_profit = min(profits)

    # shift all fitnesses to avoid negatives
    if lowest_profit < 0:
        shift = lowest_profit * -1
        
        for i in range(len(profits)):
            profits[i] += shift
    
    # use fitness to calculate probability of selection for each individual
    sum_of_profits = sum(profits)
    normalized_profits = [x / sum_of_profits for x in profits]
    
    # calculate the combined fitness of each individual
    combined_fitnesses = [(0.75 * winrate + 0.25 * normalized_profit) for winrate, normalized_profit in zip(winrates, normalized_profits)]

    # create starting and stopping points
    current_sum = 0
    stopping_point = random.random() # between 0 and 1
    
    # return random individuals based on probability
    for i, fitness in enumerate(combined_fitnesses):
        current_sum += fitness
        
        if current_sum >= stopping_point:
            return population[i]

def uniform_crossover(parent_1, parent_2):
    # child genes
    child1, child2 = [], []
    
    # get parent genes
    parent1_genes = [parent_1.timestamps, parent_1.spread_width, parent_1.num_spreads, parent_1.stop_loss, parent_1.entry_credit, parent_1.stop_price, parent_1.limit_price]
    parent2_genes = [parent_2.timestamps, parent_2.spread_width, parent_2.num_spreads, parent_2.stop_loss, parent_2.entry_credit, parent_2.stop_price, parent_2.limit_price]
    
    # randomly select genes from each parent
    for gene1, gene2 in zip(parent1_genes, parent2_genes):
        if random.random() < CROSSOVER_PROBABILITY:
            child1.append(gene1)
            child2.append(gene2)
        else:
            child1.append(gene2)
            child2.append(gene1)
            
    # create children
    child1 = Individual(*child1)
    child2 = Individual(*child2)
    return child1, child2
        
def genetic_backtest():
    # 1. generate initial population
    initial_population = generate_initial_population()
    
     # 2. calculate fitness (profit) for initial population
    initial_fitness = [individual.calculate_fitness() for individual in initial_population]
    
    # print fitness of initial population
    print("Generation 0")
    for i, fitness in enumerate(initial_fitness):
        print(f"{green}Individual {i + 1} fitness: {fitness}{reset}")
        print(f"Time: {initial_population[i].timestamps}, SW: {initial_population[i].spread_width}, NS: {initial_population[i].num_spreads}, SL: {initial_population[i].stop_loss}, EC: {initial_population[i].entry_credit}, SP: {initial_population[i].stop_price}, LP: {initial_population[i].limit_price}")
    print("")
        
    # repeat for five generations
    for x in range(GENERATIONS):
        # create new population of same size as initial
        new_population = []
        for _ in range(len(initial_population) // 2):
            # 3. select parents (biased towards those with higher fitness)
            parent_1 = select_individuals(initial_population, initial_fitness)
            parent_2 = select_individuals(initial_population, initial_fitness)
            
            # 4. combine parameters from praents to create new children
            child1, child2 = uniform_crossover(parent_1, parent_2)
            
            # 5. randomly mutate parameters for children
            child1.mutate()
            child2.mutate()
            
            # add children to new population
            new_population.append(child1)
            new_population.append(child2)
        
        # move on to next generation
        initial_population = new_population
        initial_fitness = [individual.calculate_fitness() for individual in initial_population]
        
        # print fitness of initial population
        print(f"Genearation {x + 1}")
        for y, fitness in enumerate(initial_fitness):
            print(f"{green}Individual {y + 1} fitness: {fitness}{reset}")
            print(f"Time: {initial_population[y].timestamps}, SW: {initial_population[y].spread_width}, NS: {initial_population[y].num_spreads}, SL: {initial_population[y].stop_loss}, EC: {initial_population[y].entry_credit}, SP: {initial_population[y].stop_price}, LP: {initial_population[y].limit_price}")
        print("")

#-----------------------MAIN------------------#
genetic_backtest()