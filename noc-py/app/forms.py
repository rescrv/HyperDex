from flask.ext.wtf import Form
from wtforms import TextField, IntegerField, validators
from wtforms.validators import Required, Length

class DesignSpace(Form):

	spacename = TextField('Space Name', [validators.required(), validators.Length(min=4,max=25)])
	keyname = TextField('Key Name', [validators.required(), validators.Length(min=4,max=25)])
	# attributes_count = IntegerField('Number of Attributes',[validators.required()])
	# for i in range [0,attributes_count]:
	# 	attributetype = SelectField('Attribute Type', choices=[('string','String'),
	# 		('int','Integer'),
	# 		('float', 'Float'),
	# 		('list(string)', 'String List')])

	# 	attributename = TextField('Attribute Name', [validators.Length(min=4,max=25)])
	partitions = IntegerField('Number of Partitions', [validators.required()])
	failures = IntegerField('Number of Failures to Tolerate', [validators.required()])

		# except:
		# a.add_space('''
		# 	space %s
		# 	key id
		# 	attributes
		# 		string title,
		# 		string text
		# 	subspace title
		# 	create 8 partitions
		# 	tolerate 2 failures
		# 	''' % spacename)

# class DesignAttribute(Form):

# 	attributetype = SelectField('Attribute Type', choices=[('string','String'),
# 		('int','Integer'),
# 		('float', 'Float'),
# 		('list(string)', 'String List')])

# 	attributename = TextField('Attribute Name', [validators.Length(min=4,max=25)])