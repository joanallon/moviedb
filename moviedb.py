#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from collections import Counter


# In[ ]:


class MovieDBError(ValueError):
    pass


# In[6]:


class MovieDB:

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.dir = f'{data_dir}/directors.csv'
        self.mov = f'{data_dir}/movies.csv'

    def add_movie(self, title, year, genre, director):
        add_dir = pd.DataFrame({'director_id': 1,
                                'given_name': director.split()[1].title(),
                               'last_name': director.split()[0][:-1].title(),
                                'full': director.title()}, index=[0])

# check for existing director file/append
        if not os.path.isfile(self.dir):
            add_dir.iloc[:, :3].to_csv(self.dir, index=False)
            dfd = add_dir
        else:
            dfd = pd.read_csv(self.dir)
            dfd['full'] = dfd['last_name']+', '+dfd['given_name']
            director = director.lower().strip()

            if (director not in dfd.full.str.lower().str.strip().to_list()):

                dfd = dfd.append(add_dir, ignore_index=True)
                dfd.iloc[-1, 0] = dfd.iloc[-2, 0]+1

            dfd.iloc[:, :3].to_csv(self.dir, index=False)

# create new movie row
        d_id = dfd[dfd['full'].str.lower().str.strip() ==
                   director.lower().strip()]['director_id']
        add_mov = pd.DataFrame({'movie_id': 1,
                                'title': title,
                               'year': year,
                                'genre': genre,
                                'director_id': d_id}, index=[0])

# check for existing movie file/movie or append
        if not os.path.isfile(self.mov):
            add_mov.to_csv(self.mov, index=False)
            dfm = add_mov
            return add_mov.iloc[0, 0]
        else:
            dfm = pd.read_csv(self.mov)

            if ((title.lower().strip(), year, genre.lower().strip())
                in list(zip(dfm['title'].str.lower().str.strip(),
                            dfm['year'],
                            dfm['genre'].str.lower().str.strip()))):
                raise MovieDBError('The movie is already in movies.csv.')
            else:
                dfm = dfm.append(add_mov, ignore_index=True)
                dfm.iloc[-1, 0] = dfm.iloc[-2, 0]+1
                dfm.to_csv(self.mov, index=False)
                return dfm.iloc[-1, 0]

    def add_movies(self, movies):
        success = []
        idx = 0

# check if nan or already existing
        for i in movies:
            if not os.path.isfile(self.mov):
                if len(i.values()) == 4:
                    success.append(self.add_movie(
                        i['title'], i['year'], i['genre'], i['director']))
                else:
                    print(f'Warning: movie index {idx} has invalid or'
                          ' incomplete information. Skipping...')
            else:
                dfm = pd.read_csv(self.mov)
                if len(i.values()) == 4:
                    if ((i['title'].lower().strip(),
                         i['year'],
                         i['genre'].lower().strip())
                        not in list(zip(dfm['title'].str.lower().str.strip(),
                                    dfm['year'],
                                    dfm['genre'].str.lower().str.strip()))):
                        success.append(self.add_movie(
                            i['title'], i['year'], i['genre'], i['director']))
                    else:
                        title = i['title']
                        print(f'Warning: movie {title} is already in the '
                              'database. Skipping...')

                else:
                    print(f'Warning: movie index {idx} has invalid or '
                          'incomplete information. Skipping...')
            idx += 1

        return success

    def delete_movie(self, movie_id):
        if os.path.isfile(self.mov):
            dfm = pd.read_csv(self.mov)
            if movie_id in dfm['movie_id'].to_list():
                dfm = dfm[dfm['movie_id'] != movie_id]
                dfm.to_csv(self.mov)
            else:
                raise MovieDBError('movie id not found')
        else:
            raise MovieDBError('movie id not found')

    def search_movies(self, **kwargs):

        if kwargs != {}:
            if os.path.isfile(self.mov) and os.path.isfile(self.mov):
                dfm = pd.read_csv(self.mov)
                dfd = pd.read_csv(self.dir)
                dfm = dfm.merge(dfd, on='director_id', how='outer')

                if 'title' in kwargs:
                    if (kwargs['title'].lower().strip() in (dfm['title'].str
                                                            .lower()
                                                            .str.strip().
                                                            to_list())):

                        dfm = dfm[dfm['title'].str.lower().str.strip()
                                  == kwargs['title'].lower().strip()]
                    else:
                        raise MovieDBError(
                            'Should contain at least one nontrivial argument')

                if 'year' in kwargs:
                    if kwargs['year'] in dfm['year'].to_list():
                        dfm = dfm[dfm['year'] == kwargs['year']]
                    else:
                        raise MovieDBError(
                            'Should contain at least one nontrivial argument')

                if 'genre' in kwargs:
                    if (kwargs['genre'].lower().strip() in (dfm['genre'].str
                                                            .lower()
                                                            .str.strip()
                                                            .to_list())):

                        dfm = dfm[dfm['genre'].str.lower().str.strip()
                                  == kwargs['genre'].lower().strip()]
                    else:
                        raise MovieDBError(
                            'Should contain at least one nontrivial argument')

                if 'director_id' in kwargs:
                    if kwargs['director_id'] in dfm['director_id'].to_list():
                        dfm = dfm[dfm['director_id'] == kwargs['director_id']]
                    else:
                        raise MovieDBError(
                            'Should contain at least one nontrivial argument')

                return dfm['movie_id'].to_list()

            else:
                return []
        else:
            raise MovieDBError(
                'Should contain at least one nontrivial argument')

    def export_data(self):
        if os.path.isfile(self.mov) and os.path.isfile(self.mov):
            dfm = pd.read_csv(self.mov)
            dfd = pd.read_csv(self.dir)
            dfm = dfm.merge(dfd, on='director_id',
                            how='outer').sort_values('movie_id')
            dfm.rename(columns={'given_name': 'director_given_name',
                                'last_name': 'director_last_name'},
                       inplace=True)

            return dfm[['title', 'year', 'genre', 'director_last_name',
                        'director_given_name']]
        else:
            return pd.DataFrame(columns=['title', 'year', 'genre',
                                         'director_last_name',
                                         'director_given_name'])

    def generate_statistics(self, stat):
        if stat in ['movie', 'genre', 'director', 'all']:
            if stat == 'all':
                df_all = {'movie': self.generate_statistics('movie'),
                          'genre': self.generate_statistics('genre'),
                          'director': self.generate_statistics('director')}
                return df_all
            else:
                if not os.path.isfile(self.mov):
                    return {}
                else:
                    dfm = pd.read_csv(self.mov)
                    dfd = pd.read_csv(self.dir)
                    dfd['full_name'] = dfd['last_name']+', '+dfd['given_name']
                    dfm = dfm.merge(dfd, on='director_id')

                    if stat == 'movie':
                        return dict(dfm.groupby('year')['title'].count())

                    elif stat == 'genre':
                        gen = dfm.groupby([dfm['genre'], dfm['year']])[
                            'title'].count().reset_index()
                        return dict(gen.groupby('genre')[['year', 'title']]
                                    .apply(lambda x: dict(x.values)))

                    elif stat == 'director':
                        dir_count = (dfm.groupby([dfm['full_name'],
                                                  dfm['year']])['title']
                                     .count().reset_index())
                        return dict(dir_count.groupby('full_name')
                                    [['year', 'title']].apply(lambda x:
                                                              dict(x.values)))
        else:
            raise MovieDBError('Passed stat unknown.')

    def plot_statistics(self, stat):
        dfm = pd.read_csv(self.mov)
        dfd = pd.read_csv(self.dir)
        dfm = dfm.merge(dfd, on='director_id')
        dfm['full_name'] = dfm['last_name']+', '+dfm['given_name']

        fig, ax = plt.subplots(1, 1, figsize=(8, 4))

        if stat == 'movie':
            mov = dfm.groupby('year')['title'].count()
            ax.bar(mov.index, mov.values)

        elif stat == 'genre':
            gen = (dfm.groupby(['genre', 'year'])['title'].count()
                   .reset_index().pivot(index='year', columns='genre',
                                        values='title'))
            ax.plot(gen.index, gen.values, 'o-')
            ax.legend(gen.columns, bbox_to_anchor=(1.05, 1))
        elif stat == 'director':
            top_5 = (dfm.groupby('full_name')['title'].count().reset_index()
                     .sort_values(by=['title', 'full_name'],
                                  ascending=[False, True])
                     .set_index('full_name').index[:5])
            director = (dfm.groupby(['full_name', 'year'])['title'].count()
                        .reset_index()
                        .pivot(index='year', columns='full_name',
                               values='title')
                        .loc[:, top_5])
            ax.plot(director.index, director.values, 'o-')
            ax.legend(top_5, bbox_to_anchor=(1.05, 1))
        else:
            raise MovieDBError('Passed stat is unknown.')

        ax.set_ylabel('movies')
        plt.show()
        return ax

    def token_freq(self):
        if os.path.isfile(self.dir) and os.path.isfile(self.mov):
            dfm = pd.read_csv(self.mov)
            title_list = dfm['title'].str.split().to_list()
            word_list = [i.casefold() for sub in title_list for i in sub]

            return Counter(word_list)
        else:
            return {}
