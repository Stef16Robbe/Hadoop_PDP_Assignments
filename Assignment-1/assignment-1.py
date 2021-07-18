from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies, combiner=self.combiner_count_ratings, reducer=self.reducer_sum_rating_counts),
            MRStep(reducer=self.reducer_sort_movies_by_ratings)
        ]

    # get all movie ids
    def mapper_get_movies(self, _, line):
        (_, movieID, _, _) = line.split('\t')
        yield movieID, 1
    
    # combine the count of ratings with their movie ids
    def combiner_count_ratings(self, movie_id, ratings):
        yield movie_id, sum(ratings)
    
    # sum the count of the ratings
    def reducer_sum_rating_counts(self, movie_id, ratings):
        yield None, (sum(ratings), movie_id)

    # sort the movies based on their ratings    
    def reducer_sort_movies_by_ratings(self, _, movies):
        for count, movie_id in sorted(movies):
            yield (int(movie_id), int(count))

if __name__ == "__main__":
    Ratings.run()
