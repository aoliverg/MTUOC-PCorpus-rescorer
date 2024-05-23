import fasttext
import sys

training_data=sys.argv[1]
model_name=sys.argv[2]

# Train the model
model = fasttext.train_supervised(input=training_data, epoch=25, lr=1.0, wordNgrams=2, verbose=2, minCount=1)

# Save the model
model.save_model(model_name)