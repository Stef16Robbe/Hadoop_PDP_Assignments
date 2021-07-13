from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies, combiner=self.combiner_count_ratings, reducer=self.reducer_sum_rating_counts),
            MRStep(reducer=self.reducer_sort_movies_by_ratings)
        ]

    def mapper_get_movies(self, _, line):
        (_, movieID, _, _) = line.split('\t')
        yield movieID, 1
    
    def combiner_count_ratings(self, movie_id, ratings):
        yield movie_id, sum(ratings)
    
    def reducer_sum_rating_counts(self, movie_id, ratings):
        yield None, (sum(ratings), movie_id)
    
    def reducer_sort_movies_by_ratings(self, _, movies):
        for count, movie_id in sorted(movies):
            yield (int(movie_id), int(count))

if __name__ == "__main__":
    Ratings.run()
