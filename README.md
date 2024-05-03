# Sending Confirmation Emails

This Ruby on Rails project focuses on setting up confirmation emails to be sent out when users book tickets in the Flight Booker application. The confirmation email will be sent to each passenger individually as part of the booking process.

## Setup Instructions

Follow these steps to set up and run the project locally:

### Prerequisites

Make sure you have the following installed:

- Ruby (version 2.7.0 or higher)
- Rails (version 6.0.0 or higher)
- Git

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Ismat-Samadov/Confirmation_Emails.git
   ```

2. Navigate into the project directory:

   ```bash
   cd Confirmation_Emails
   ```

3. Install dependencies:

   ```bash
   bundle install
   ```

4. Set up the mailer and configure the email provider (e.g., SendGrid) for production deployment.

### Usage

To run the application, start the Rails server:

```bash
rails server
```

The application will be available at `http://localhost:3000`.

### How to Use

1. Ensure that the mailer is set up correctly by generating the PassengerMailer with:

   ```bash
   rails generate mailer PassengerMailer
   ```

2. Install the `letter_opener` gem to view emails in the browser during development:

   ```ruby
   gem 'letter_opener', group: :development
   ```

   Then run:

   ```bash
   bundle install
   ```

3. Follow the steps outlined in the Rails Guide to create the action to send the confirmation email. Build both `.html.erb` and `.text.erb` versions of the ticket confirmation email.

4. Test the email sending functionality by creating a new flight booking. `letter_opener` should open the email in the browser if configured properly.

5. Optionally, you can test the mailer directly from the Rails console:

   ```ruby
   PassengerMailer.confirmation_email(Passenger.first).deliver_now!
   ```

### Extra Credit

Deploy the application to a hosting provider and test the email functionality in a production environment. Additional setup may be required to configure an email provider like SendGrid for sending emails in production.

## Acknowledgements

This project is part of the Ruby on Rails Course by [The Odin Project](https://www.theodinproject.com/). Special thanks to the instructors for providing the guidance and resources.

## Additional Resources

- [letter_opener docs](https://github.com/ryanb/letter_opener)
- Setting up email: Rails, Heroku, SendGrid, Figaro