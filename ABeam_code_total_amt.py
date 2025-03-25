import apache_beam as beam

p1 = beam.Pipeline()

Total_amount = (
  p1
    | "Import Customer Data" >> beam.io.ReadFromText(r"C:\Users\GG196QU\OneDrive - EY\Documents\py\ApacheBeam\Customers.txt")
    | "Split by comma" >> beam.Map(lambda record: record.split(','))
    | "Create a key-value pair" >> beam.Map(lambda record: (record[0], (int(record[2]), int(record[3]))))
    | "GroupBy Customer" >> beam.GroupByKey()
    | "Calculate Amount" >> beam.Map(
        lambda customer_data:(
            customer_data[0],
            sum(amount for amount, x in customer_data[1]),  # Sum the items purchased
            sum(amount for x, amount in customer_data[1])   # Total amount by each customer
        )
    )
    | "Print Results" >> beam.Map(print)
)
p1.run()