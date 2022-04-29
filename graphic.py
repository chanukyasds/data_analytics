from data_processor import preprocessed_data
import matplotlib.pyplot as plt

data_frame = preprocessed_data()
data_frame['collections'] = data_frame['collections'].astype('int64')
data_frame.plot(x='year', y='collections', kind="bar")
print("plotting the data on bar graph...")
plt.show()

