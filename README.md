<p align="center">
  <img src="miscellaneous/databricks_logo.png" width="450"/>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <h1 align="center">StripeX Lakehouse End-to-End Medallion Pipeline</h1>
</p>
<br>

<h2 align="center">Project Background</h2>
<p><strong>StripeX</strong> is an online footwear retailer. The dataset we use in this project is synthetic data we will generate it by using Python Faker library. To understand our dataset better, we need to know from which sources we collect them. The data comes from 5 different source system:</p>
<p>
  <img src="miscellaneous/stripex_data_source.png" width="1000"/>
</p>
<ul>
  <li><strong>Order management DB:</strong> transactional OLTP database. Tuned for fast small writes, can not be used for analytical workload</li>
  <li><strong>Customer platform:</strong> transactional OLTP database. Same as Order Management DB.</li>
  <li><strong>Product catalog:</strong> CSV export files. Data changes less frequently.</li>
  <li><strong>Website clickstream:</strong> raw JSON customer events on StripeX website.</li>
  <li><strong>Inventory:</strong> nightly inventory CSV snapshot files.</li>
  
</ul>
<br>
<br>
<h2 align="center">Problems we solve in this project:</h2>
<br>
<p><strong>1.Finance:</strong> in StripeX company the revenue is a hand-stitched spreadsheets, a day late and open to human error. The team needs to export data from 3 different sources and compile it together manually and make it ready to the VP of Finance every morning.</p>
<br>
<p><strong>2.Marketing:</strong> in company nobody can answer to "which customer about to churn? Or From which marketing channel did customer heard about the company?". This questions can't be answered without pulling 3 people into the room and a half day of exporting and cross-referencing multiple Excel sheets.</p>
<br>
<p><strong>3.Merchandising:</strong> reordering the stock is often based on a gut feel and a monthly Excel report. These reports are already stale by the time anyone reads it and you can't always trust them. Popular shoe brands and sizes getting out of stock, unpopular brands are lying in the dead stock.</p>
<br>
<p>
  <img src="miscellaneous/stripex_problems.jpg" width="1000"/>
</p>
